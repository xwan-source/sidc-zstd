#include "containerstore.h"
#include "../utils/serial.h"
#include "../utils/sync_queue.h"
#include "../jcr.h"
#include "../destor.h"
#include "../common.h"
#include "../index/index.h"
//#include <zstd.h>


static int64_t container_count = 0;
static FILE* fp;
/* Control the concurrent accesses to fp. */
static pthread_mutex_t mutex;

static pthread_t append_t;

static SyncQueue* container_buffer;

/*
 * We must ensure a container is either in the buffer or written to disks.
 */
static void* append_thread(void *arg) {

	while (1) {
		struct container *c = sync_queue_get_top(container_buffer);
		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		write_container(c);
		TIMER_END(1, jcr.write_time);
		sync_queue_pop(container_buffer);
		
		free_container(c);
	}

	assert(fflush(fp)==0);
	assert(fsync(fileno(fp))==0);
	
	return NULL;
}

void init_container_store() {

	sds containerfile = sdsdup(destor.working_directory);
	containerfile = sdscat(containerfile, "/container.pool");

	if ((fp = fopen(containerfile, "r+"))) {
		fread(&container_count, 8, 1, fp);
	} else if (!(fp = fopen(containerfile, "w+"))) {
		perror(
				"Can not create containers/container.pool for read and write because");
		exit(1);
	}

	sdsfree(containerfile);

	container_buffer = sync_queue_new(25);
	pthread_mutex_init(&mutex, NULL);
	pthread_create(&append_t, NULL, append_thread, NULL);
}

void close_container_store() {
	sync_queue_term(container_buffer);
	pthread_join(append_t, NULL);

	fseek(fp, 0, SEEK_SET);
	fwrite(&container_count, sizeof(container_count), 1, fp);

	fclose(fp);
	fp = NULL;

	pthread_mutex_destroy(&mutex);
}

static void init_container_meta(struct containerMeta *meta) {
	meta->chunk_num = 0;
	meta->data_size = 0;
	meta->id = TEMPORARY_ID;
	meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);
}

struct container* create_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	c->data = calloc(1, CONTAINER_SIZE);

	init_container_meta(&c->meta);
	c->meta.id = container_count++;
	return c;
}

containerid get_container_id(struct container* c) {
	return c->meta.id;
}

void write_container_async(struct container* c) {
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld", c->meta.id);
		return;
	}
	sync_queue_push(container_buffer, c);
}

static inline void mark_bitmap(unsigned char* bitmap, int n){
	/* mark the nth bit */
	bitmap[n >> 3] |= (1 << (n & 7));
}

/*
 * Called by Append phase
 * HEAD, bitmap, meta entries, data
 */
void write_container(struct container* c) {

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs at the end of a backup */
		container_count--;
		VERBOSE("Append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	VERBOSE("Append phase: Writing container %lld of %d chunks", c->meta.id,
			c->meta.chunk_num);

	unsigned char *bitmap = calloc((c->meta.chunk_num+7)/8, 1);
	unsigned char *entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);
	int n = 0;

	ser_declare;
	ser_begin(entries, 0);
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, c->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry *me = (struct metaEntry *) value;
		ser_bytes(&me->fp, sizeof(fingerprint));						// 20B
		ser_bytes(&me->offset, sizeof(int32_t));						// 4B
		ser_bytes(&me->data_len, sizeof(int32_t));						// 4B
		ser_bytes(&me->base_fp, sizeof(fingerprint));					// 20B
		ser_bytes(&me->base_size, sizeof(int32_t));						// 4B
		ser_bytes(&me->sf1, sizeof(uint64_t));							// 8B
		ser_bytes(&me->sf2, sizeof(uint64_t));							// 8B
		ser_bytes(&me->sf3, sizeof(uint64_t));							// 8B

		if(me->flag)
			mark_bitmap(bitmap, n);
		n++;
	}
	ser_end(entries, c->meta.chunk_num * CONTAINER_META_ENTRY);
	
	pthread_mutex_lock(&mutex);
	
	if (fseek(fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
		perror("Fail seek in container store.");
		exit(1);
	}

    assert(c->meta.id != TEMPORARY_ID);

	/* container head */
	int count = fwrite(&c->meta.id, sizeof(c->meta.id), 1, fp);					//8B
	count += fwrite(&c->meta.chunk_num, sizeof(c->meta.chunk_num), 1, fp);		//4B
	count += fwrite(&c->meta.data_size, sizeof(c->meta.data_size), 1, fp);		//4B

	count += fwrite(bitmap, (c->meta.chunk_num+7)/8, 1, fp);
	count += fwrite(entries, c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);

    /* CONTAINER_HEAD includes containerid(8B), chunk_num(4B), and data_size(4B) */
	count += fwrite(c->data, CONTAINER_SIZE - CONTAINER_HEAD - (c->meta.chunk_num+7)/8
			- c->meta.chunk_num*CONTAINER_META_ENTRY, 1, fp);
	assert(count == 6);
	
	pthread_mutex_unlock(&mutex);

	free(entries);
	free(bitmap);
}

struct containerMeta* load_container_meta_by_id(containerid id) {
	struct containerMeta* cm = NULL;

	/* First, we find it in the buffer */
	cm = sync_queue_find(container_buffer, container_check_id, &id,
			container_meta_duplicate);
	if (cm)
		return cm;

    cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_container_meta(cm);

	pthread_mutex_lock(&mutex);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);

	int count = fread(&cm->id, sizeof(cm->id), 1, fp);
	if(cm->id != id){
		WARNING("expect %lld, but read %lld", id, cm->id);
		assert(cm->id == id);
	}

	count += fread(&cm->chunk_num, sizeof(cm->chunk_num), 1, fp);
	count += fread(&cm->data_size, sizeof(cm->data_size), 1, fp);

	unsigned char *bitmap = malloc((cm->chunk_num+7)/8);
	unsigned char *entries = malloc(cm->chunk_num * CONTAINER_META_ENTRY);

	count += fread(bitmap, (cm->chunk_num+7)/8, 1, fp);
	count += fread(entries, cm->chunk_num * CONTAINER_META_ENTRY, 1, fp);

	assert(count == 5);

	pthread_mutex_unlock(&mutex);

	unser_declare;
	unser_begin(entries, 0);

	int i;
	for (i = 0; i < cm->chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));						// 20B
		unser_bytes(&me->offset, sizeof(int32_t));						// 4B
		unser_bytes(&me->data_len, sizeof(int32_t));					// 4B
		unser_bytes(&me->base_fp, sizeof(fingerprint));					// 20B
		unser_bytes(&me->base_size, sizeof(int32_t));					// 4B
		unser_bytes(&me->sf1, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf2, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf3, sizeof(uint64_t));						// 8B

		me->flag = 0;
        me->delta_size = -1;

		if (bitmap[i>>3] & (1 << (i & 7))) {
			/* it is a delta */
			me->flag = 1;
			me->delta_size = me->data_len - 12;
		}

		g_hash_table_insert(cm->map, &me->fp, me);
	}

	unser_end(entries, cm->chunk_num * CONTAINER_META_ENTRY);

	free(entries);
	free(bitmap);

	return cm;
}

struct container* load_container_by_id(containerid id) {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	init_container_meta(&c->meta);
	c->data = malloc(CONTAINER_SIZE);

	pthread_mutex_lock(&mutex);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);
	int count = fread(&c->meta.id, sizeof(c->meta.id), 1, fp);

	if(c->meta.id != id){
		WARNING("expect %lld, but read %lld", id, c->meta.id);
        WARNING("current container number is %d", container_count);
		assert(c->meta.id == id);
	}

	count += fread(&c->meta.chunk_num, sizeof(c->meta.chunk_num), 1, fp);
	count += fread(&c->meta.data_size, sizeof(c->meta.data_size), 1, fp);

	unsigned char *bitmap = malloc((c->meta.chunk_num+7)/8);
	unsigned char *entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);

	count += fread(bitmap, (c->meta.chunk_num+7)/8, 1, fp);
	count += fread(entries, c->meta.chunk_num * CONTAINER_META_ENTRY, 1, fp);

	count += fread(c->data, CONTAINER_SIZE - CONTAINER_HEAD - (c->meta.chunk_num+7)/8
			- c->meta.chunk_num*CONTAINER_META_ENTRY, 1, fp);
	assert(count == 6);

	pthread_mutex_unlock(&mutex);

	unser_declare;
	unser_begin(entries, 0);

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));						// 20B
		unser_bytes(&me->offset, sizeof(int32_t));						// 4B
		unser_bytes(&me->data_len, sizeof(int32_t));					// 4B
		unser_bytes(&me->base_fp, sizeof(fingerprint));					// 20B
		unser_bytes(&me->base_size, sizeof(int32_t));					// 4B
		unser_bytes(&me->sf1, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf2, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf3, sizeof(uint64_t));						// 8B
		unser_bytes(&me->flag, sizeof(char));							// 1B
		unser_bytes(&me->chunk_len, sizeof(int32_t));					// 4B

		me->delta_size = -1;
		if (me->flag == 1){
			/* it is a delta */
			me->delta_size = me->data_len - sizeof(int32_t) - sizeof(containerid);
		}

		g_hash_table_insert(c->meta.map, &me->fp, me);
	}
	unser_end(entries, c->meta.chunk_num * CONTAINER_META_ENTRY);

	free(entries);
	free(bitmap);
	
	return c;
}

struct container* get_container_in_write_buffer(containerid id) {
    struct container* con = NULL;
    con = sync_queue_find(container_buffer, container_check_id, &id, container_duplicate);

    return con;
}

struct container* load_container_for_meta_and_chunk(containerid id) {

	struct container *c = NULL;

	/* First, we find it in the write buffer */
	c = sync_queue_find(container_buffer, container_check_id, &id,
			container_duplicate);
	if (c)
		return c;

    c = (struct container*) malloc(sizeof(struct container));
	init_container_meta(&c->meta);
	c->data = malloc(CONTAINER_SIZE);

	unsigned char buf[CONTAINER_SIZE];
	unsigned char *ptr = NULL;

	pthread_mutex_lock(&mutex);

	fseek(fp, id * CONTAINER_SIZE + 8, SEEK_SET);
	fread(buf, CONTAINER_SIZE, 1, fp);

	pthread_mutex_unlock(&mutex);

	ptr = buf;
	memcpy(&c->meta.id, ptr, sizeof(c->meta.id));
	ptr += sizeof(c->meta.id);
	
	if(c->meta.id != id){
		WARNING("expect %lld, but read %lld", id, c->meta.id);
		assert(c->meta.id == id);
	}
	
	memcpy(&c->meta.chunk_num, ptr, sizeof(c->meta.chunk_num));
	ptr += sizeof(c->meta.chunk_num);
	
	memcpy(&c->meta.data_size, ptr, sizeof(c->meta.data_size));
	ptr += sizeof(c->meta.data_size);
	
	unsigned char *bitmap = malloc((c->meta.chunk_num+7)/8);
	unsigned char *entries = malloc(c->meta.chunk_num * CONTAINER_META_ENTRY);
	
	memcpy(bitmap, ptr, (c->meta.chunk_num+7)/8);
	ptr += (c->meta.chunk_num+7)/8;
	
	memcpy(entries, ptr, c->meta.chunk_num * CONTAINER_META_ENTRY);
	ptr += c->meta.chunk_num * CONTAINER_META_ENTRY;
	
	memcpy(c->data, ptr, CONTAINER_SIZE - CONTAINER_HEAD - (c->meta.chunk_num+7)/8
			        - c->meta.chunk_num*CONTAINER_META_ENTRY);

	unser_declare;
	unser_begin(entries, 0);

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));

		unser_bytes(&me->fp, sizeof(fingerprint));						// 20B
		unser_bytes(&me->offset, sizeof(int32_t));						// 4B
		unser_bytes(&me->data_len, sizeof(int32_t));					// 4B
		unser_bytes(&me->base_fp, sizeof(fingerprint));					// 20B
		unser_bytes(&me->base_size, sizeof(int32_t));					// 4B
		unser_bytes(&me->sf1, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf2, sizeof(uint64_t));						// 8B
		unser_bytes(&me->sf3, sizeof(uint64_t));						// 8B
		unser_bytes(&me->flag, sizeof(char));							// 1B
		unser_bytes(&me->chunk_len, sizeof(int32_t));					// 4B

		me->base_id = -1;
		me->delta_size = -1;

		if (me->flag == 1) {
			/* it is a delta */
			me->delta_size = me->data_len - sizeof(int32_t) - sizeof(containerid);
		}
		
		g_hash_table_insert(c->meta.map, &me->fp, me);
	}
	unser_end(entries, c->meta.chunk_num * CONTAINER_META_ENTRY);
		
	free(entries);
	free(bitmap);
	
	return c;
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

int check_container_in_list(struct container *con, int64_t cid) {
    return (con->meta.id == cid) ?  1 : 0;
}

struct chunk* get_chunk_in_container(struct container* c, fingerprint *fp) {
	struct metaEntry* me = get_meta_entry_in_container_meta(&c->meta, fp);
	assert(me);

	struct chunk* ck = NULL;

	//printf("cid: %d, offset: %d, a %s, data lenght: %d\n", c->meta.id, me->offset, (me->flag == 1)?"delta":"chunk", me->data_len);

	if(me->flag){
		/* A delta */
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

		int32_t chunk_size = me->data_len;
		ck = new_chunk(chunk_size);
	
		unser_declare;
		unser_begin(c->data + me->offset, 0);
		ck = new_chunk(chunk_size);
		unser_bytes(ck->data, chunk_size);
		unser_end(c->data + me->offset, chunk_size);
	}
	memcpy(&ck->fp, fp, sizeof(fingerprint));
	ck->id = c->meta.id;
	assert(ck->id != -1);
		
	return ck;
}

struct chunk* get_base_chunk_in_container(struct container* c, fingerprint *fp) {

	struct metaEntry* me = get_meta_entry_in_container_meta(&c->meta, fp);
	assert(me);
    assert(me->flag == 0);

	struct chunk* ck = NULL;
	/*
	char code[41];
	hash2code(*fp, code);
	code[40] = 0;
	*/
	//printf("cid: %d, offset: %d, a base, data lenght: %d\n", c->meta.id, me->offset, me->data_len);
	/*
	int32_t rSize = ZSTD_getFrameContentSize(c->data + me->offset, me->data_len);
	assert(rSize != ZSTD_CONTENTSIZE_ERROR);
    assert(rSize != ZSTD_CONTENTSIZE_UNKNOWN);
	unsigned char* restoreBuffer = malloc(rSize);

	int32_t dSize = ZSTD_decompress(restoreBuffer, rSize, 
						c->data + me->offset, me->data_len);
	assert(!ZSTD_isError(dSize));
	*/
	ck = new_chunk(me->data_len);
	
	unser_declare;
	unser_begin(c->data + me->offset, 0);
	unser_bytes(ck->data, me->data_len);
	unser_end(c->data + me->offset, me->data_len);
	
	memcpy(&ck->fp, fp, sizeof(fingerprint));
	ck->id = c->meta.id;
	assert(ck->id != -1);

	ck->target_size_for_inversed_compression = me->data_len;

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

int add_chunk_to_container(struct container* c, struct chunk* ck) {
	//assert(!container_overflow(c, ck));
	struct metaEntry* me_t = g_hash_table_lookup(c->meta.map, &ck->fp);
	
	if (me_t) {
		//NOTICE("Writing a chunk already in the container buffer!");
		ck->id = c->meta.id;

		return 0;
	}
    if (!ck->delta) {
		/* store as a normal chunk */

		//assert(ck->local_compressed_contents);
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
		me->offset = c->meta.data_size;
    	me->delta_size = 0;
    	me->base_size = 0;

		me->flag = 0;
		
		me->sf1 = ck->sketches->sf1;
		me->sf2 = ck->sketches->sf2;
		me->sf3 = ck->sketches->sf3;

		memcpy(c->data + c->meta.data_size, ck->data, ck->size);
		me->data_len = ck->size;
		c->meta.data_size += me->data_len;

		g_hash_table_insert(c->meta.map, &me->fp, me);

		c->meta.chunk_num++;

		ck->dup_with_delta = 0;
		ck->delta_compressed = 0;
		ck->base_id = -1;
		
    }
    else {
		/* store as a delta */
		jcr.delta_compressed_chunk_num++;
		
		assert(ck->delta);
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
		memcpy(&me->base_fp, &ck->delta->base_fp, sizeof(fingerprint));
		me->offset = c->meta.data_size;
    	me->delta_size = ck->delta->size;
    	me->base_size = ck->delta->base_size;
		me->base_id = ck->delta->base_id;
		
		me->flag = 1;

		ser_declare;
		ser_begin(c->data + c->meta.data_size, 0);
		ser_bytes(&ck->size, sizeof(int32_t));					// add chunk size (4B)
		ser_bytes(&ck->delta->base_id, sizeof(containerid));	// add container id of its base chunk (8B)
		ser_bytes(ck->delta->data, ck->delta->size);			// add delta data
		ser_end(c->data + c->meta.data_size, sizeof(int32_t) + sizeof(containerid) + ck->delta->size);
		me->data_len = sizeof(int32_t) + sizeof(containerid) + ck->delta->size;
		c->meta.data_size += me->data_len;
		
		g_hash_table_insert(c->meta.map, &me->fp, me);

		c->meta.chunk_num++;

		ck->dup_with_delta = 0;
		ck->delta_compressed = 1;
		ck->base_id = ck->delta->base_id;
		ck->stored_as_delta = 1;
	}
	ck->id = c->meta.id;

	return 1;
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

int64_t get_container_count()
{
    return container_count;
}
