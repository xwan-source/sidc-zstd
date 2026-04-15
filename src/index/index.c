#include "index.h"
#include "kvstore.h"
#include "fingerprint_cache.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "../jcr.h"
#include "../destor.h"
#include "../utils/rabin_chunking.h"
#include "db_mysql.h"
#include "../utils/bloom_filter.h"


//#include "simi_detection.h"

/* The buffer size > 2 * destor.rewrite_buffer_size */
/* All fingerprints that have been looked up in the index
 * but not been updated. */

struct {
    /* map a fingerprint to a queue of indexElem */
    /* Index all fingerprints in the index buffer. */
    GHashTable *buffered_fingerprints;
    /* The number of buffered chunks */
    int chunk_num;
} index_buffer;

struct {
    /* Requests to the key-value store */
    int lookup_requests;
    int update_requests;
    int lookup_requests_for_unique;
    /* Overheads of prefetching module */
    int read_prefetching_units;
}index_overhead;

static unsigned char* filter;
static int dirty = FALSE;
int64_t index_memory_overhead = 0;

gboolean g_feature_equal(char* a, char* b){
	return !memcmp(a, b, destor.index_key_size);
}

guint g_feature_hash(char *feature){
	int i, hash = 0;
	for(i=0; i<destor.index_key_size; i++){
		hash += feature[i] << (8*i);
	}
	return hash;
}

void init_index() {
    /* Do NOT assign a free function for value. */

    index_buffer.buffered_fingerprints = g_hash_table_new_full(g_int64_hash,
            g_fingerprint_equal, NULL, NULL);
    index_buffer.chunk_num = 0;

	if (db_init() == FALSE) {
		return FALSE;
	}
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/bloom_filter");
	int fd;
	if ((fd = open(indexpath, O_RDONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/bloom_filter!");
		return FALSE;
	}
	filter = malloc(FILTER_SIZE_BYTES);
	if (FILTER_SIZE_BYTES != read(fd, filter, FILTER_SIZE_BYTES)) {
		bzero(filter, FILTER_SIZE_BYTES);
	}
	close(fd);
	dirty = FALSE;

    if(destor.index_specific != INDEX_SPECIFIC_NO){
        destor.index_key_size = sizeof(fingerprint);
        switch(destor.index_specific){
            case INDEX_SPECIFIC_DDFS:{
                destor.index_category[0] = INDEX_CATEGORY_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
                break;
            }
            case INDEX_SPECIFIC_BLOCK_LOCALITY_CACHING:{
                destor.index_category[0] = INDEX_CATEGORY_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;
                destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
                destor.index_sampling_method[1] = 1;

                destor.index_segment_prefech = destor.index_segment_prefech > 1 ?
                destor.index_segment_prefech : 16;
                break;
            }
            case INDEX_SPECIFIC_SAMPLED:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;

                destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
                destor.index_sampling_method[1] = destor.index_sampling_method[1] > 1 ?
                destor.index_sampling_method[1] : 128;
                break;
            }
            case INDEX_SPECIFIC_SPARSE:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;

                destor.index_segment_algorithm[0] = INDEX_SEGMENT_CONTENT_DEFINED;

                destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;

                destor.index_sampling_method[0] = INDEX_SAMPLING_RANDOM;
                destor.index_sampling_method[1] = destor.index_sampling_method[1] > 1 ?
                destor.index_sampling_method[1] : 128;

                destor.index_segment_prefech = 1;
                break;
            }
            case INDEX_SPECIFIC_SILO:{
                destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
                destor.index_category[1] = INDEX_CATEGORY_LOGICAL_LOCALITY;

                destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;

                destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
                destor.index_segment_selection_method[1] = 1;

                destor.index_sampling_method[0] = INDEX_SAMPLING_MIN;
                destor.index_sampling_method[1] = 0;

                destor.index_segment_prefech = destor.index_segment_prefech > 1 ?
                destor.index_segment_prefech : 16;
                break;
            }
            default:{
                WARNING("Invalid index specific!");
                exit(1);
            }
        }
    }

    if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
        destor.index_key_size = sizeof(fingerprint);
    }

    if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){
        destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
        if(destor.index_category[0] == INDEX_CATEGORY_EXACT){
            destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
            destor.index_sampling_method[1] = 1;
        }
    }

    assert(destor.index_key_size > 0 && destor.index_key_size <= sizeof(fingerprint));

    init_sampling_method();
    init_segmenting_method();

    init_kvstore();

    init_fingerprint_cache();

    index_overhead.lookup_requests = 0;
    index_overhead.update_requests = 0;
    index_overhead.lookup_requests_for_unique = 0;
    index_overhead.read_prefetching_units = 0;
}

void close_index() {
    close_kvstore();
	full_index_destroy();
}

extern struct{
    /* accessed in dedup phase */
    struct container *container_buffer;
    /* In order to facilitate sampling in container,
     * we keep a queue for chunks in container buffer. */
    GQueue *chunks;
} storage_buffer;

void lookup_base(struct segment *s) {
    int len = g_queue_get_length(s->chunks), i;

    for (i = 0; i < len; ++i) {
        struct chunk* c = g_queue_peek_nth(s->chunks, i);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

		/* First check it in the storage buffer */
        if(storage_buffer.container_buffer
                && delta_lookup_fingerprint_in_container(storage_buffer.container_buffer, c)){
            c->id = get_container_id(storage_buffer.container_buffer);
            SET_CHUNK(c, CHUNK_DUPLICATE);
            SET_CHUNK(c, CHUNK_REWRITE_DENIED);
        }

		/* Then check the buffered fingerprints which are recently processed during backup.*/
        GQueue *tq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
        if (!tq) {
            tq = g_queue_new();
        }
        else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			
            struct indexElem *be = g_queue_peek_head(tq);
            c->id = be->id;
			c->base_size = be->base_size;

			if(be->delta_compressed || be->dup_with_delta) {
				c->dup_with_delta = 1;
				c->base_id = be->base_id;
				SET_CHUNK(c, CHUNK_SELF_REFERENCE);
				memcpy(&c->base_fp, &be->base_fp, sizeof(fingerprint));

				check_baseid_in_fingerprint_cache_lookup(c);
			}
            SET_CHUNK(c, CHUNK_DUPLICATE);
        }

		/*  Check the fingerprint cache */
        if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
            /* Searching in fingerprint cache */
            int64_t id = delta_fingerprint_cache_lookup(c);
            if(id != TEMPORARY_ID){
                c->id = id;
                SET_CHUNK(c, CHUNK_DUPLICATE);
            }
        }

		/*  Finally, check the bloom filter and mysql database */
       	if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)){
      		if (in_dict(filter, (char*)&c->fp, sizeof(fingerprint))) {
				TIMER_DECLARE(2);
	            TIMER_BEGIN(2);
				containerid cid = db_lookup_fingerprint(&c->fp);
				TIMER_END(2, jcr.mysql_lookup_time);
				jcr.mysql_lookup_times++;

				if (cid != TEMPORARY_ID) {
					index_overhead.lookup_requests++;
                	/* prefetch the target unit */
                	index_overhead.read_prefetching_units++;

                	fingerprint_cache_prefetch(cid);
					jcr.prefetch_base_size += CONTAINER_SIZE;

                	int64_t id = delta_fingerprint_cache_lookup(c);
                	if (id != TEMPORARY_ID){
                    	c->id = id;
                    	SET_CHUNK(c, CHUNK_DUPLICATE);
                	}
                	else {
                    	NOTICE("Filter phase: A key collision occurs");
                	}
				}
				else {
                	index_overhead.lookup_requests_for_unique++;
                	VERBOSE("Dedup phase: non-existing fingerprint");
            	}
			}
       	}

		/* Insert it into the index buffer */
        struct indexElem *ne = (struct indexElem*)malloc(sizeof(struct indexElem));
        ne->id = c->id;
        memcpy(&ne->fp, &c->fp, sizeof(fingerprint));
        ne->base_id = c->base_id;
        ne->base_size = c->base_size;
        ne->delta_size = c->delta_size;
		ne->dup_with_delta = c->dup_with_delta;
		ne->delta_compressed = 0;

        g_queue_push_tail(tq, ne);
        g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, tq);

        index_buffer.chunk_num++;
    }
}

extern void index_lookup_similarity_detection(struct segment *s);

extern struct {
    GMutex mutex;
    GCond cond; // index buffer is not full

    int wait_threshold;
} index_lock;

/*
 * return 1: indicates lookup is successful.
 * return 0: indicates the index buffer is full.
 */
int index_lookup(struct segment* s) {

	/* Ensure the next phase not be blocked. */
    if (index_lock.wait_threshold > 0
            && index_buffer.chunk_num >= index_lock.wait_threshold) {
        DEBUG("The index buffer is full (%d chunks in buffer)",
                index_buffer.chunk_num);
        return 0;
    }

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

//    if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY
//        && destor.index_segment_selection_method[0] != INDEX_SEGMENT_SELECT_BASE) {
//        s->features = sampling(s->chunks,s->chunk_num);//提取特征
//        index_lookup_similarity_detection(s);
//    }else{
//        lookup_base(s);
//    }
    lookup_base(s);

    TIMER_END(1, jcr.dedup_time);

    return 1;
}

/* Input features with an ID */
void index_update(GHashTable *features, int64_t id){
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, features);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        index_overhead.update_requests++;
		db_insert_fingerprint(key, id);
		insert_word(filter, (char*)key, sizeof(fingerprint));
		dirty = TRUE;
    }
}

inline void index_delete(fingerprint *fp, int64_t id){
	//kvstore_delete(fp, id);
}

void bloom_filter_flush() {
	if (dirty == FALSE)
		return;
	/* flush bloom filter */
	sds indexpath = sdsdup(destor.working_directory);
	indexpath = sdscat(indexpath, "index/bloom_filter");

	int fd;
	if ((fd = open(indexpath, O_WRONLY | O_CREAT, S_IRWXU)) <= 0) {
		printf("Can not open index/bloom_filter!");
	}
	if (FILTER_SIZE_BYTES != write(fd, filter, FILTER_SIZE_BYTES)) {
		printf("%s, %d: Failed to flush bloom filter!\n", __FILE__, __LINE__);
	}
	close(fd);

	dirty = FALSE;
	sdsfree(indexpath);
}

void full_index_destroy() {
	index_memory_overhead = db_close(); // one byte for each chunk
	bloom_filter_flush();
	free(filter);
}

int index_buffer_detect(struct chunk *ck) {
	if(ck->id == -1 
		|| (ck->delta && ck->delta->base_id == -1) 
		|| ck->base_id == -1) {

		struct indexElem *be = g_hash_table_lookup(index_buffer.buffered_fingerprints, &ck->fp);
		if(be && (be->id != -1)) {
			ck->id = be->id;
            ck->base_id = be->base_id;
			ck->base_size = be->base_size;
            ck->dup_with_delta = be->dup_with_delta;
			
            SET_CHUNK(ck, CHUNK_DUPLICATE);
			SET_CHUNK(ck, CHUNK_SELF_REFERENCE);
			
			memcpy(&ck->base_fp, &be->base_fp, sizeof(fingerprint));
    	}
	}
}

void index_buffer_update_normal(struct chunk* ck) {

	struct indexElem *ne = (struct indexElem*)malloc(sizeof(struct indexElem));
    ne->id = ck->id;
    memcpy(&ne->fp, &ck->fp, sizeof(fingerprint));
    ne->base_id = ck->base_id;
    ne->base_size = ck->base_size;
    ne->delta_size = ck->delta_size;
	ne->dup_with_delta = ck->dup_with_delta;
	//if(ck->delta_compressed)
	//		ne->dup_with_delta = 1;
	memcpy(&ne->base_fp, &ck->base_fp, sizeof(fingerprint));
		
    g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, ne);
}

void index_buffer_update_inverse(struct chunk* ck) {

	struct indexElem *ne = (struct indexElem*)malloc(sizeof(struct indexElem));
    ne->id = ck->id;
    memcpy(&ne->fp, &ck->delta->fp, sizeof(fingerprint));
    ne->base_id = ck->id;
    ne->base_size = ck->size;
    ne->delta_size = ck->delta->size;
	ne->dup_with_delta = 1;

	memcpy(&ne->base_fp, &ck->fp, sizeof(fingerprint));
		
    g_hash_table_replace(index_buffer.buffered_fingerprints, &ne->fp, ne);
}

/* This function is designed for rewriting. */
void index_check_buffer(struct segment *s) {

    int len = g_queue_get_length(s->chunks), i;

    for (i = 0; i < len; ++i) {
        struct chunk* c = g_queue_peek_nth(s->chunks, i);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        if((CHECK_CHUNK(c, CHUNK_DUPLICATE) && c->id == TEMPORARY_ID)
                || CHECK_CHUNK(c, CHUNK_OUT_OF_ORDER)
                || CHECK_CHUNK(c, CHUNK_SPARSE)
                || c->dup_with_delta){
            /*
             * 1. A duplicate chunk with a TEMPORARY_ID,
             * 	  indicates it is a copy of a recent unique chunk.
             * 2. If we want to rewrite a chunk,
             *    we check whether it has been rewritten recently.
             */
            GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);
            assert(bq);
            struct indexElem* be = g_queue_peek_head(bq);

            if(be->id > c->id){

                if(c->id != TEMPORARY_ID) {
                    /* this chunk has been rewritten recently
                     * thus, if the current chunk has id, we need to deny the rewrite */
                    SET_CHUNK(c, CHUNK_REWRITE_DENIED);
                }

                c->id = be->id;
				c->base_id = be->base_id;
                c->base_size = be->base_size;
                c->delta_size = be->delta_size;

				if (be->delta_compressed) {
					c->dup_with_delta = 1;
					memcpy(&c->base_fp, &be->base_fp, sizeof(fingerprint));
				}
				else
					c->dup_with_delta = 0;
				
            }
        }
    }
}

int index_update_buffer(struct segment *s) {

	int len = g_queue_get_length(s->chunks), i;
    for (i = 0; i < len; ++i) {
        struct chunk* c = g_queue_peek_nth(s->chunks, i);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        assert(c->id != TEMPORARY_ID);

        GQueue* bq = g_hash_table_lookup(index_buffer.buffered_fingerprints, &c->fp);

        struct indexElem* e = g_queue_pop_head(bq);
        if(g_queue_get_length(bq) == 0){
            g_hash_table_remove(index_buffer.buffered_fingerprints, &c->fp);
            g_queue_free(bq);
        }
        else{
            int num = g_queue_get_length(bq), j;
            for(j=0; j<num; j++){
                /*
                 * set all container id in this buck equal to the cid of the first stored,
                 * note that if the current c is not the first stored chunk, it's cid will equals
                 * to the first stored chunk due to the recently_rewritten_chunks in filter_phase
                 */
                struct indexElem* ie = g_queue_peek_nth(bq, j);
                ie->id = c->id;
                ie->base_size = c->base_size;
                ie->delta_size = c->delta_size;

                if (c->delta_compressed || c->dup_with_delta) {

					//assert(c->base_id != -1);
					ie->base_id = c->base_id;
                    //ie->dup_with_delta = 1;
					memcpy(&ie->base_fp, &c->base_fp, sizeof(fingerprint));
                }
				else
					ie->dup_with_delta = 0;
            }
        }
        free(e);
        index_buffer.chunk_num--;
    }

    if (index_lock.wait_threshold <= 0
            || index_buffer.chunk_num < index_lock.wait_threshold) {
        DEBUG("The index buffer is ready for more chunks (%d chunks in buffer)",
                index_buffer.chunk_num);
        return 0;
    }
    return 1;
}
