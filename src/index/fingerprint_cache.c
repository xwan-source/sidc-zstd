/*
 * fingerprint_cache.c
 *
 *  Created on: Mar 24, 2014
 *      Author: fumin
 */
#include "../destor.h"
#include "index.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "../utils/lru_cache.h"
#include "base_container_cache.h"
#include "../jcr.h"

static GHashTable *prefetched_cid;

extern int dense_container_check(containerid cid);

static struct lruCache* lru_queue;

void init_fingerprint_cache(){
	prefetched_cid = g_hash_table_new_full(
			g_int64_hash, g_int64_equal, free, NULL);

	switch(destor.index_category[1]){
	case INDEX_CATEGORY_PHYSICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);
		break;
	case INDEX_CATEGORY_LOGICAL_LOCALITY:
		lru_queue = new_lru_cache(destor.index_cache_size,
				free_segment_recipe, lookup_fingerprint_in_segment_recipe);
		break;
	default:
		WARNING("Invalid index category!");
		exit(1);
	}
}

int64_t fingerprint_cache_lookup(fingerprint *fp){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta* cm = lru_cache_lookup(lru_queue, fp);
			if (cm)
				return cm->id;
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			struct segmentRecipe* sr = lru_cache_lookup(lru_queue, fp);
			if(sr){
				struct chunkPointer* cp = g_hash_table_lookup(sr->kvpairs, fp);
				if(cp->id <= TEMPORARY_ID){
					WARNING("expect > TEMPORARY_ID, but being %lld", cp->id);
					assert(cp->id > TEMPORARY_ID);
				}
				return cp->id;
			}
			break;
		}
	}

	return TEMPORARY_ID;
}

int64_t delta_fingerprint_cache_lookup(struct chunk* c){

    struct containerMeta* cm = lru_cache_lookup(lru_queue, &c->fp);
	if (cm) {
        struct metaEntry* me = g_hash_table_lookup(cm->map, &c->fp);
        assert(me);
        if (me->flag) {
            c->base_size = me->base_size;
            c->delta_size = me->delta_size;
			c->dup_with_delta = 1;
			memcpy(&c->base_fp, &me->base_fp, sizeof(fingerprint));

			jcr.detected_duplicate_delta_num++;

			struct containerMeta* base_cm = lru_cache_lookup_without_update(lru_queue, &me->base_fp);
			if (base_cm) {
				struct metaEntry* base_me = g_hash_table_lookup(base_cm->map, &me->base_fp);
        		if(base_me && (base_me->flag == 0 || base_me->flag == 2)) {
					assert(base_cm->id != -1);
					c->base_id = base_cm->id;
					jcr.base_chunk_in_fcache++;
        		}
        	}
        }
		return cm->id;
	}
	return TEMPORARY_ID;
}

void check_baseid_in_fingerprint_cache_lookup(struct chunk* c) {

    struct containerMeta* cm = lru_cache_lookup_without_update(lru_queue, &c->base_fp);
	if (cm) {
		struct metaEntry* me = g_hash_table_lookup(cm->map, &c->base_fp);
        if(me && (me->flag == 0 || me->flag == 2)) {
			assert(cm->id != -1);
			c->base_id = cm->id;
			jcr.base_chunk_in_fcache++;
        }
		else
			SET_CHUNK(c, CHUNK_BASE_FRAGMENTED);
   	}
	else
		SET_CHUNK(c, CHUNK_BASE_FRAGMENTED);
}


void fingerprint_cache_prefetch(int64_t id){
	switch(destor.index_category[1]){
		case INDEX_CATEGORY_PHYSICAL_LOCALITY:{
			struct containerMeta * cm = NULL;

			TIMER_DECLARE(1);
	        TIMER_BEGIN(1);
			cm = load_container_meta_by_id(id);
			TIMER_END(1, jcr.prefetch_index_time);
			jcr.prefetch_index_size += 64*1024;
			jcr.prefetch_index_times++;

			if (cm) {
				lru_cache_insert(lru_queue, cm, NULL, NULL);
				similarity_index_cache_update(cm);
			}
			else {
				WARNING("Error! The container %lld has not been written!", id);
				exit(1);
			}
			break;
		}
		case INDEX_CATEGORY_LOGICAL_LOCALITY:{
			GQueue* segments = prefetch_segments(id,
					destor.index_segment_prefech);
			VERBOSE("Dedup phase: prefetch %d segments into %d cache",
					g_queue_get_length(segments),
					destor.index_cache_size);
			struct segmentRecipe* sr;
			while ((sr = g_queue_pop_tail(segments))) {
				/* From tail to head */
				if (!lru_cache_hits(lru_queue, &sr->id,
						segment_recipe_check_id)) {
					lru_cache_insert(lru_queue, sr, NULL, NULL);
				} else {
					 /* Already in cache */
					free_segment_recipe(sr);
				}
			}
			g_queue_free(segments);
			break;
		}
	}
}
