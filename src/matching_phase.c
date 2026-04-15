/*
 * matching_phase.c
 *
 *  Created on: July 23, 2018
 *      Author: Yucheng Zhang
 */

#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "storage/containerstore.h"
#include "index/base_container_cache.h"

extern int sparse_container_check(containerid id);

static pthread_mutex_t mutex;
static pthread_t matching_t;
static GHashTable *containernum_base_referenced;

static void* matching_thread(void *arg) {

	while (1) {
        struct chunk* c = sync_queue_pop(resemblance_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        if(CHECK_CHUNK(c, CHUNK_FILE_START) ||
        		CHECK_CHUNK(c, CHUNK_FILE_END) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_START) ||
        		CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
        	sync_queue_push(matching_queue, c);
        	continue;
        }

        if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {

        	struct chunk* baseChunk = NULL;
        	if (c->size	> destor.mini_sketch_size) {
				TIMER_DECLARE(1);
				TIMER_BEGIN(1);
            	baseChunk = detect_similar_chunk(c);
				TIMER_END(1, jcr.sketch_lookup_time);
				jcr.sketch_lookup_size += c->size;
        	}

        	if (baseChunk) {
				c->base_chunk = baseChunk;
				assert(baseChunk->id != -1);
        	}
        	else {
				c->delta = NULL;
            	c->base_chunk = NULL;
        	}
        	sync_queue_push(matching_queue, c);
        }
		else {
			sync_queue_push(matching_queue, c);
        	continue;
		}
    }

    sync_queue_term(matching_queue);
	close_base_container_cache();

	return NULL;
}

void start_matching_phase() {

    init_base_container_cache();
	containernum_base_referenced = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);

	matching_queue = sync_queue_new(2000);
	pthread_create(&matching_t, NULL, matching_thread, NULL);
}

void stop_matching_phase() {
	pthread_join(matching_t, NULL);
	jcr.base_container_referenced = g_hash_table_size(containernum_base_referenced);
	g_hash_table_destroy(containernum_base_referenced);
}
