#include "containerstore.h"
#include "jcr.h"
#include "index.h"
#include "storage.h"
#include "utils/serial.h"
#include "utils/sds.h"
#include "local_index.h"
#include <zstd.h>

/* zstd 压缩级别 */
#define ZSTD_COMPRESSION_LEVEL 3

extern int64_t index_memory_overhead;

/* 
 * 使用 zstd 压缩数据
 * 返回压缩后的大小，如果失败返回 -1
 */
int zstd_compress(unsigned char* dest, int dest_len, 
                  unsigned char* src, int src_len) {
    size_t const compressed_size = ZSTD_compress(dest, dest_len,
                                                  src, src_len, 
                                                  ZSTD_COMPRESSION_LEVEL);
    if (ZSTD_isError(compressed_size)) {
        fprintf(stderr, "ZSTD compression failed: %s\n", 
                ZSTD_getErrorName(compressed_size));
        return -1;
    }
    return (int)compressed_size;
}

/* 
 * 使用 zstd 解压缩数据
 * 返回解压缩后的大小，如果失败返回 -1
 */
int zstd_decompress(unsigned char* dest, int dest_len,
                    unsigned char* src, int src_len) {
    size_t const decompressed_size = ZSTD_decompress(dest, dest_len,
                                                      src, src_len);
    if (ZSTD_isError(decompressed_size)) {
        fprintf(stderr, "ZSTD decompression failed: %s\n",
                ZSTD_getErrorName(decompressed_size));
        return -1;
    }
    return (int)decompressed_size;
}

static pthread_t containerstore_t;

/* 
 * 容器元数据序列化后的长度
 * 用于计算序列化后的元数据大小
 */
int container_meta_entry_size(struct containerMeta* cm) {
	return CONTAINER_META_ENTRY * cm->chunk_num;
}

void init_container_meta(struct containerMeta* cm) {
	cm->id = TEMPORARY_ID;
	cm->chunk_num = 0;
	cm->data_size = 0;
	cm->map = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free);
}

void init_container(struct container* c) {
	init_container_meta(&c->meta);
	c->data = calloc(1, CONTAINER_SIZE);
}

struct container* new_container() {
	struct container* c = (struct container*) malloc(sizeof(struct container));
	init_container(c);
	return c;
}

void free_container_meta(struct containerMeta* cm) {
	g_hash_table_destroy(cm->map);
	free(cm);
}

void free_container(struct container* c) {
	if(c->meta.map) {
		g_hash_table_destroy(c->meta.map);
		free(c->meta.map = NULL);
	}
	if (c->data)
		free(c->data);
	free(c);
}

int container_empty(struct container* c) {
	return c->meta.chunk_num == 0 ? 1 : 0;
}

/* Return 0 if doesn't exist. */
int lookup_fingerprint_in_container_meta(struct containerMeta* cm,
		fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp) == NULL ? 0 : 1;
}

int lookup_fingerprint_in_container(struct container* c, fingerprint *fp) {
	return lookup_fingerprint_in_container_meta(&c->meta, fp);
}

int delta_lookup_fingerprint_in_container(struct container* c, struct chunk* ck) {
	int res = 0;
	struct metaEntry* me = g_hash_table_lookup(c->meta.map, ck->fp);
	if (me) {
		if (me->flag) {
			assert(me->base_id != -1);
			ck->base_id= me->base_id;
            ck->base_size = me->base_size;
            ck->delta_size = me->delta_size;
			ck->dup_with_delta = 1;
		}
		res = 1;
	}
	return res;
}

gint g_container_cmp_desc(struct container* c1, struct container* c2,
		gpointer user_data) {
	return g_container_meta_cmp_desc(&c1->meta, &c2->meta, user_data);
}

gint g_container_meta_cmp_desc(struct containerMeta* cm1,
		struct containerMeta* cm2, gpointer user_data) {
	return cm2->id - cm1->id;
}

int container_check_id(struct container* c, containerid* id) {
	return container_meta_check_id(&c->meta, id);
}

int container_meta_check_id(struct containerMeta* cm, containerid* id) {
	return cm->id == *id ? 1 : 0;
}

/*
 * foreach the fingerprints in the container.
 * Apply the 'func' for each fingerprint.
 */
void container_meta_foreach(struct containerMeta* cm, void (*func)(fingerprint*, void*), void* data){
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, cm->map);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		func(key, data);
	}
}

void write_container(struct container* c, int option) {
	assert(c->meta.chunk_num > 0);
	assert(c->meta.data_size > 0);

	/* Open the container file */
	sds file = sdsdup(destor.working_directory);
	file = sdscat(file, option == CONTAINER_DELTA ? "container.delta.pool/" : "container.pool/");
	file = sdscatprintf(file, "%d", c->meta.id);

	FILE *fp;
	if((fp = fopen(file, "w+")) == NULL){
		fprintf(stderr, "fopen container file %s error: %s\n", file, strerror(errno));
		exit(1);
	}

	/* 
	 * Container metadata format:
	 * | id (8 bytes) | chunk_num (4 bytes) | data_size (4 bytes) |
	 * | bitmap (variable) |
	 * | entries (variable) |
	 * | data (variable) |
	 */

	/* Write container header */
	fwrite(&c->meta.id, sizeof(containerid), 1, fp);
	fwrite(&c->meta.chunk_num, sizeof(int32_t), 1, fp);
	fwrite(&c->meta.data_size, sizeof(int32_t), 1, fp);

	/* Write bitmap and entries */
	unsigned char* bitmap = calloc(1, (c->meta.chunk_num + 7) / 8);
	unsigned char* entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);
	memset(entries, 0, c->meta.chunk_num * CONTAINER_META_ENTRY);

	GHashTableIter iter;
	gpointer key, value;
	int i = 0;
	g_hash_table_iter_init(&iter, c->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		fingerprint* fp_key = (fingerprint*) key;
		struct metaEntry* me = (struct metaEntry*) value;

		bitmap[i / 8] |= (1 << (i % 8));

		unsigned char* entry_ptr = entries + i * CONTAINER_META_ENTRY;
		ser_declare;
		ser_begin(entry_ptr, 0);
		ser_bytes(fp_key, sizeof(fingerprint));
		ser_bytes(&me->offset, sizeof(int32_t));
		ser_bytes(&me->data_len, sizeof(int32_t));
		ser_bytes(&me->flag, sizeof(int8_t));
		ser_bytes(&me->chunk_len, sizeof(int32_t));
		ser_bytes(&me->base_id, sizeof(containerid));
		ser_bytes(&me->delta_size, sizeof(int32_t));
		ser_bytes(&me->base_size, sizeof(int32_t));
		ser_bytes(&me->base_fp, sizeof(fingerprint));
		ser_bytes(&me->sf1, sizeof(int32_t));
		ser_bytes(&me->sf2, sizeof(int32_t));
		ser_bytes(&me->sf3, sizeof(int32_t));
		ser_end(entry_ptr, CONTAINER_META_ENTRY);

		i++;
	}

	assert(i == c->meta.chunk_num);

	fwrite(bitmap, 1, (c->meta.chunk_num + 7) / 8, fp);
	fwrite(entries, CONTAINER_META_ENTRY, c->meta.chunk_num, fp);

	/* Write data */
	fwrite(c->data, 1, c->meta.data_size, fp);

	free(bitmap);
	free(entries);
	fclose(fp);

	sdsfree(file);
}

struct containerMeta* read_container_meta(FILE *fp) {
	struct containerMeta* cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_container_meta(cm);

	/* Read header */
	fread(&cm->id, sizeof(containerid), 1, fp);
	fread(&cm->chunk_num, sizeof(int32_t), 1, fp);
	fread(&cm->data_size, sizeof(int32_t), 1, fp);

	/* Read bitmap */
	unsigned char* bitmap = malloc((cm->chunk_num + 7) / 8);
	fread(bitmap, 1, (cm->chunk_num + 7) / 8, fp);

	/* Read entries */
	for (int i = 0; i < cm->chunk_num; i++) {
		unsigned char entry[CONTAINER_META_ENTRY];
		fread(entry, CONTAINER_META_ENTRY, 1, fp);

		if (bitmap[i / 8] & (1 << (i % 8))) {
			struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));

			unser_declare;
			unser_begin(entry, 0);
			unser_bytes(&me->fp, sizeof(fingerprint));
			unser_bytes(&me->offset, sizeof(int32_t));
			unser_bytes(&me->data_len, sizeof(int32_t));
			unser_bytes(&me->flag, sizeof(int8_t));
			unser_bytes(&me->chunk_len, sizeof(int32_t));
			unser_bytes(&me->base_id, sizeof(containerid));
			unser_bytes(&me->delta_size, sizeof(int32_t));
			unser_bytes(&me->base_size, sizeof(int32_t));
			unser_bytes(&me->base_fp, sizeof(fingerprint));
			unser_bytes(&me->sf1, sizeof(int32_t));
			unser_bytes(&me->sf2, sizeof(int32_t));
			unser_bytes(&me->sf3, sizeof(int32_t));
			unser_end(entry, CONTAINER_META_ENTRY);

			g_hash_table_insert(cm->map, &me->fp, me);
		}
	}

	free(bitmap);
	return cm;
}

struct container* read_container(FILE *fp, int option) {
	struct container* c = new_container();

	/* Read metadata */
	c->meta = *read_container_meta(fp);

	/* Read data */
	fread(c->data, 1, c->meta.data_size, fp);

	return c;
}

struct containerMeta* retrieve_container_meta_by_id(containerid id, int option) {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	sds file = sdsdup(destor.working_directory);
	file = sdscat(file, option == CONTAINER_DELTA ? "container.delta.pool/" : "container.pool/");
	file = sdscatprintf(file, "%d", id);

	FILE *fp;
	if((fp = fopen(file, "r")) == NULL){
		fprintf(stderr, "fopen container file %s error: %s\n", file, strerror(errno));
		exit(1);
	}

	struct containerMeta* cm = read_container_meta(fp);
	fclose(fp);

	sdsfree(file);

	TIMER_END(1, jcr.read_container_time);

	return cm;
}

struct container* retrieve_container_by_id(containerid id, int option) {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	sds file = sdsdup(destor.working_directory);
	file = sdscat(file, option == CONTAINER_DELTA ? "container.delta.pool/" : "container.pool/");
	file = sdscatprintf(file, "%d", id);

	FILE *fp;
	if((fp = fopen(file, "r")) == NULL){
		fprintf(stderr, "fopen container file %s error: %s\n", file, strerror(errno));
		exit(1);
	}

	struct container* c = read_container(fp, option);
	fclose(fp);

	sdsfree(file);

	TIMER_END(1, jcr.read_container_time);

	return c;
}

struct containerMeta* retrieve_container_meta_by_id_from_remote(containerid id, int option) {
	/* 
	 * TODO: Implement remote retrieval
	 * For now, just call local retrieval
	 */
	return retrieve_container_meta_by_id(id, option);
}

struct container* retrieve_container_by_id_from_remote(containerid id, int option) {
	/* 
	 * TODO: Implement remote retrieval
	 * For now, just call local retrieval
	 */
	return retrieve_container_by_id(id, option);
}

void add_chunk_to_container(struct container* c, struct chunk* ck) {
	if (ck->delta) {
		/* Delta chunk */
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
		me->offset = c->meta.data_size;
		me->flag = 0;  /* Delta chunk */
		me->base_id = ck->delta->base_id;
		me->delta_size = ck->delta->size;
		me->base_size = ck->delta->base_size;
		memcpy(&me->base_fp, &ck->delta->base_fp, sizeof(fingerprint));
		me->chunk_len = 0;
		me->sf1 = 0;
		me->sf2 = 0;
		me->sf3 = 0;

		/* Serialize delta chunk data */
		ser_declare;
		ser_begin(c->data + c->meta.data_size, 0);
		ser_bytes(&ck->size, sizeof(int32_t));
		ser_bytes(&ck->delta->base_id, sizeof(containerid));
		ser_bytes(ck->delta->data, ck->delta->size);
		ser_end(c->data + c->meta.data_size, ck->delta->size + sizeof(int32_t) + sizeof(containerid));

		me->data_len = ck->delta->size + sizeof(int32_t) + sizeof(containerid);
		c->meta.data_size += me->data_len;

		g_hash_table_insert(c->meta.map, &me->fp, me);

		c->meta.chunk_num++;

		ck->dup_with_delta = 1;
		ck->delta_compressed = 1;
	} else {
		/* Normal chunk - compress with zstd during storage */
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
		me->offset = c->meta.data_size;
    	me->delta_size = 0;
    	me->base_size = 0;

		/* 普通块使用 zstd 压缩存储 */
		me->flag = 1;
		me->sf1 = ck->sketches->sf1;
		me->sf2 = ck->sketches->sf2;
		me->sf3 = ck->sketches->sf3;

		/* 使用 zstd 压缩数据 */
		size_t const compressed_buff_size = ZSTD_compressBound(ck->size);
		void* compressed_buffer = malloc(compressed_buff_size);
		if (compressed_buffer == NULL) {
			fprintf(stderr, "Failed to allocate memory for container compression\n");
			exit(1);
		}

		size_t const compressed_size = ZSTD_compress(compressed_buffer, compressed_buff_size,
		                                          ck->data, ck->size, 3);
		if (ZSTD_isError(compressed_size)) {
			fprintf(stderr, "ZSTD compression failed in container: %s\n", ZSTD_getErrorName(compressed_size));
			free(compressed_buffer);
			exit(1);
		}

		/* 只有当压缩后大小小于原始大小时才使用压缩数据 */
		if (compressed_size < ck->size) {
			/* 存储压缩后的数据 */
			memcpy(c->data + c->meta.data_size, compressed_buffer, compressed_size);
			me->data_len = compressed_size;
			me->chunk_len = ck->size;  /* 存储原始大小用于解压 */
		} else {
			/* 压缩后反而更大，存储原始数据 */
			memcpy(c->data + c->meta.data_size, ck->data, ck->size);
			me->data_len = ck->size;
			me->chunk_len = 0;  /* 0 表示未压缩 */
		}
		free(compressed_buffer);
		c->meta.data_size += me->data_len;

		g_hash_table_insert(c->meta.map, &me->fp, me);

		c->meta.chunk_num++;

		ck->dup_with_delta = 0;
		ck->delta_compressed = 0;
	}
}

struct chunk* get_chunk_in_container(struct container* c, fingerprint *fp) {
	struct metaEntry* me = get_meta_entry_in_container_meta(&c->meta, fp);
	assert(me);

	struct chunk* ck = NULL;

	//printf("cid: %d, offset: %d, a %s, data lenght: %d\n", c->meta.id, me->offset, (me->flag == 1)?"delta":"chunk", me->data_len);

	if(me->flag == 0){
		/* 差量块 (flag == 0)，需要差量解码 */
		int32_t chunk_size = 0;
		int32_t delta_size = me->data_len - sizeof(int32_t) - sizeof(containerid);
	
		unser_declare;
		unser_begin(c->data + me->offset, 0);
		unser_bytes(&chunk_size, sizeof(int32_t));

		ck = new_chunk(chunk_size);
		ck->delta = new_delta(delta_size);
		
		unser_bytes(&ck->delta->base_id, sizeof(containerid));
		unser_bytes(ck->delta->data, delta_size);
		unser_end(c->data + me->offset, me->data_len);
		
		memcpy(&ck->delta->base_fp, &me->base_fp, sizeof(fingerprint));
		memcpy(&ck->base_fp, &me->base_fp, sizeof(fingerprint));

		ck->delta->base_size = me->base_size;
		ck->base_size = ck->delta->base_size;
		ck->base_id = ck->delta->base_id;
	}
	else {
		/* 普通块（flag == 1），需要解本地压缩 */
		int32_t compressed_size = me->data_len;
		int32_t original_size = me->chunk_len;  /* chunk_len 存储原始大小 */

		/* 分配解压缓冲区 */
		unsigned char* decompressed_data = (unsigned char*)malloc(original_size);
		if (decompressed_data == NULL) {
			fprintf(stderr, "Failed to allocate memory for decompression\n");
			exit(1);
		}

		/* 从 container 读取压缩数据 */
		unsigned char* compressed_data = c->data + me->offset;

		/* 使用 zstd 解压 */
		int decompressed_len = zstd_decompress(compressed_data, compressed_size,
		                          decompressed_data, original_size);
		if (decompressed_len != original_size) {
			fprintf(stderr, "ZSTD decompression failed: expected=%d, got=%d\n",
			        original_size, decompressed_len);
			free(decompressed_data);
			exit(1);
		}

		/* 创建 chunk */
		ck = new_chunk(original_size);
		memcpy(ck->data, decompressed_data, original_size);
		free(decompressed_data);
	}
	memcpy(&ck->fp, fp, sizeof(fingerprint));
	ck->id = c->meta.id;
	assert(ck->id != -1);
		
	return ck;
}

struct chunk* get_base_chunk_in_container(struct container* c, fingerprint *fp) {

	struct metaEntry* me = get_meta_entry_in_container_meta(&c->meta, fp);
	assert(me);

	struct chunk* ck = NULL;

	if (me->flag == 1) {
		/* 普通块（flag == 1），需要解本地压缩 */
		int32_t compressed_size = me->data_len;
		int32_t original_size = me->chunk_len;  /* chunk_len 存储原始大小 */

		/* 分配解压缓冲区 */
		unsigned char* decompressed_data = (unsigned char*)malloc(original_size);
		if (decompressed_data == NULL) {
			fprintf(stderr, "Failed to allocate memory for base chunk decompression\n");
			exit(1);
		}

		/* 从 container 读取压缩数据 */
		unsigned char* compressed_data = c->data + me->offset;

		/* 使用 zstd 解压 */
		int decompressed_len = zstd_decompress(compressed_data, compressed_size,
		                          decompressed_data, original_size);
		if (decompressed_len != original_size) {
			fprintf(stderr, "ZSTD decompression failed for base chunk: expected=%d, got=%d\n",
			        original_size, decompressed_len);
			free(decompressed_data);
			exit(1);
		}

		/* 创建 chunk */
		ck = new_chunk(original_size);
		memcpy(ck->data, decompressed_data, original_size);
		free(decompressed_data);

		ck->target_size_for_inversed_compression = original_size;
	} else {
		/* 差量块不能作为基块 */
		fprintf(stderr, "Error: delta chunk cannot be used as base chunk (flag=%d)\n", me->flag);
		exit(1);
	}

	memcpy(&ck->fp, fp, sizeof(fingerprint));
	ck->id = c->meta.id;
	assert(ck->id != -1);

	return ck;
}

static inline int calculate_meta_size(int chunk_num) {

	int size = CONTAINER_HEAD;
	size += (chunk_num + 7) / 8;
	size += chunk_num * CONTAINER_META_ENTRY;
	
	return size;
}

int container_overflow(struct container* c, struct chunk* ck) {

	int capacity = c->meta.data_size;
	int chunk_num = 1;

    if(!ck->delta) {
		
        capacity += ck->size;
    }
    else 
		capacity += ck->delta->size + sizeof(int32_t) + sizeof(containerid);

    capacity += calculate_meta_size(c->meta.chunk_num + chunk_num);

	if(capacity > CONTAINER_SIZE)
		return 1;
	return 0;
}

struct metaEntry* get_meta_entry_in_container_meta(struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct container* container_duplicate(struct container *c) {
	struct container* base = c;
	struct container* dup = (struct container*) malloc(sizeof(struct container));
    dup->data = calloc(1, CONTAINER_SIZE);
	init_container_meta(&dup->meta);

	dup->meta.id = base->meta.id;
	dup->meta.chunk_num = base->meta.chunk_num;
	dup->meta.data_size = base->meta.data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->meta.map, &me->fp, me);
	}

    memcpy(dup->data, base->data, CONTAINER_SIZE);
	
	return dup;
}

struct container* retrieve_container_in_write_buffer(containerid id) {
    struct container* con = NULL;
    con = sync_queue_find(container_buffer, container_check_id, &id,
			container_duplicate);

    if (con)
        return con;
}

struct metaEntry* get_meta_entry_in_container_meta(struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

int container_overflow(struct container* c, struct chunk* ck) {

	int capacity = c->meta.data_size;
	int chunk_num = 1;

    if(!ck->delta) {
		
        capacity += ck->size;
    }
    else 
		capacity += ck->delta->size + sizeof(int32_t) + sizeof(containerid);

    capacity += calculate_meta_size(c->meta.chunk_num + chunk_num);

	if(capacity > CONTAINER_SIZE)
		return 1;
	return 0;
}

struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct container* container_duplicate(struct container *c) {
	struct container* base = c;
	struct container* dup = (struct container*) malloc(sizeof(struct container));
    dup->data = calloc(1, CONTAINER_SIZE);
	init_container_meta(&dup->meta);

	dup->meta.id = base->meta.id;
	dup->meta.chunk_num = base->meta.chunk_num;
	dup->meta.data_size = base->meta.data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->meta.map, &me->fp, me);
	}

    memcpy(dup->data, base->data, CONTAINER_SIZE);
	
	return dup;
}

struct container* retrieve_container_in_write_buffer(containerid id) {
    struct container* con = NULL;
    con = sync_queue_find(container_buffer, container_check_id, &id,
			container_duplicate);

    if (con)
        return con;
}

struct metaEntry* get_meta_entry_in_container_meta(struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct container* container_duplicate(struct container *c) {
	struct container* base = c;
	struct container* dup = (struct container*) malloc(sizeof(struct container));
    dup->data = calloc(1, CONTAINER_SIZE);
	init_container_meta(&dup->meta);

	dup->meta.id = base->meta.id;
	dup->meta.chunk_num = base->meta.chunk_num;
	dup->meta.data_size = base->meta.data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->meta.map, &me->fp, me);
	}

    memcpy(dup->data, base->data, CONTAINER_SIZE);
	
	return dup;
}

struct container* retrieve_container_in_write_buffer(containerid id) {
    struct container* con = NULL;
    con = sync_queue_find(container_buffer, container_check_id, &id,
			container_duplicate);

    if (con)
        return con;
}

struct metaEntry* get_meta_entry_in_container_meta(struct containerMeta* cm, fingerprint *fp) {
	return g_hash_table_lookup(cm->map, fp);
}

struct containerMeta* container_meta_duplicate(struct container *c) {
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct container* container_duplicate(struct container *c) {
	struct container* base = c;
	struct container* dup = (struct container*) malloc(sizeof(struct container));
    dup->data = calloc(1, CONTAINER_SIZE);
	init_container_meta(&dup->meta);

	dup->meta.id = base->meta.id;
	dup->meta.chunk_num = base->meta.chunk_num;
	dup->meta.data_size = base->meta.data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->meta.map, &me->fp, me);
	}

    memcpy(dup->data, base->data, CONTAINER_SIZE);
	
	return dup;
}

struct container* retrieve_container_in_write_buffer(containerid id) {
    struct container* con = NULL;
    con = sync_queue_find(container_buffer, container_check_id, &id,
			container_duplicate);

    if (con)
        return con;
}
