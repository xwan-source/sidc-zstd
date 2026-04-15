/*
 * har_rewrite.c
 *
 *  Created on: Nov 27, 2013
 *      Author: fumin
 */
#include "destor.h"
#include "rewrite_phase.h"
#include "storage/containerstore.h"
#include "jcr.h"
#include "cma.h"
#include "index/index.h"

static pthread_mutex_t mutex;

static GHashTable *container_utilization_monitor;
static GHashTable *inherited_rewrite_containers;
static GHashTable *inherited_dense_containers;
static GHashTable *inherited_sparse_containers;


void init_har() {

    pthread_mutex_init(&mutex, NULL);

	/* Monitor the utilizations of containers */
	container_utilization_monitor = g_hash_table_new_full(
			g_int64_hash, g_int64_equal, NULL, free);

    inherited_rewrite_containers = g_hash_table_new_full(g_int64_hash,
			g_int64_equal, free, NULL);

	inherited_dense_containers = g_hash_table_new_full(g_int64_hash,
			g_int64_equal, free, NULL);

	inherited_sparse_containers = g_hash_table_new_full(g_int64_hash,
			g_int64_equal, free, NULL);

	/* The first backup doesn't have inherited sparse containers. */
	if (jcr.id > 0) {

        sds fname = sdsdup(destor.working_directory);
		fname = sdscat(fname, "recipes/bv");
		char s[20];
		sprintf(s, "%d", jcr.id - 1);
		fname = sdscat(fname, s);
		fname = sdscat(fname, ".rewrite");

		FILE* rewrite_file = fopen(fname, "r");

		if (rewrite_file) {
			char buf[128];
			while (fgets(buf, 128, rewrite_file) != NULL) {
                containerid* p_cid = (containerid*)malloc(sizeof(containerid));
                int32_t size;
				sscanf(buf, "%lld %d", p_cid, &size);

				g_hash_table_insert(inherited_rewrite_containers, p_cid,
						NULL);
			}
			fclose(rewrite_file);
		}
		sdsfree(fname);

		sds f_DenseContainer = sdsdup(destor.working_directory);
		f_DenseContainer = sdscat(f_DenseContainer, "denseContainers/bv");
		f_DenseContainer = sdscat(f_DenseContainer, s);
		f_DenseContainer = sdscat(f_DenseContainer, ".dense");

		FILE* dense_file =  fopen(f_DenseContainer, "r");

		if (dense_file) {
			char buf[128];
			while (fgets(buf, 128, dense_file) != NULL) {
                containerid* p_cid = (containerid*)malloc(sizeof(containerid));
                int32_t size;
				sscanf(buf, "%lld %d", p_cid, &size);

				g_hash_table_insert(inherited_dense_containers, p_cid,
						NULL);
			}
			fclose(dense_file);
		}
		sdsfree(f_DenseContainer);

		sds f_sparseContainer = sdsdup(destor.working_directory);
		f_sparseContainer = sdscat(f_sparseContainer, "denseContainers/bv");
		f_sparseContainer = sdscat(f_sparseContainer, s);
		f_sparseContainer = sdscat(f_sparseContainer, ".sparse");

		FILE* sparse_file =  fopen(f_sparseContainer, "r");

		if (sparse_file) {
			char buf[128];
			while (fgets(buf, 128, dense_file) != NULL) {
                containerid* p_cid = (containerid*)malloc(sizeof(containerid));
                int32_t size;
				sscanf(buf, "%lld %d", p_cid, &size);

				g_hash_table_insert(inherited_sparse_containers, p_cid,
						NULL);
			}
			fclose(sparse_file);
		}
		sdsfree(f_sparseContainer);
	}
}

void har_monitor_update(struct chunk* c) {

	if(c->dup_with_delta) {
		//assert(c->base_id != -1);
		har_monitor_update_dupDelta(c->base_id, c->base_size);
	}
	else if (c->delta) {
		//assert(c->id != -1);
		har_monitor_update_delta(c->id, c->delta->size);
		//assert(c->delta->base_id != -1);
		har_monitor_update_base(c->delta->base_id, c->delta->base_size);
	}
	else {
		//assert(c->id != -1);
		har_monitor_update_chunk(c->id, c->size);
	}
}

void har_monitor_update_chunk(containerid id, int32_t chunkSize) {

	assert(id != -1);
	struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor, &id);

	if (record) {
        record->chunk_size += chunkSize;
	}
    else {
        record = (struct containerRecord*) malloc(sizeof(struct containerRecord));

		record->chunk_size = chunkSize;
		record->delta_size = 0;
        record->base_size = 0;
		record->cid = id;

        g_hash_table_insert(container_utilization_monitor,
		    &record->cid, record);
    }
}

void har_monitor_update_delta(containerid id, int32_t deltaSize) {

	assert(id != -1);
	struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor, &id);

	if (record) {
        record->delta_size += deltaSize;
	}
    else {
        record = (struct containerRecord*) malloc(sizeof(struct containerRecord));

		record->chunk_size = 0;
		record->delta_size = deltaSize;
        record->base_size = 0;
		record->cid = id;

        g_hash_table_insert(container_utilization_monitor,
		    &record->cid, record);
    }
}

void har_monitor_update_base(containerid base_id, int32_t baseSize) {

	assert(base_id != -1);
	struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor, &base_id);

	if (record) {
        record->base_size += baseSize;
	}
    else {
        record = (struct containerRecord*) malloc(sizeof(struct containerRecord));

		record->chunk_size = 0;
		record->delta_size = 0;
        record->base_size = baseSize;
		record->cid = base_id;

        g_hash_table_insert(container_utilization_monitor,
		    &record->cid, record);
    }
}

void har_monitor_update_dupDelta(containerid id, int32_t baseSize) {

	assert(id != -1);
    struct containerRecord* record = g_hash_table_lookup(
			container_utilization_monitor, &id);

	if (record) {
        record->base_size += baseSize;
    }
    else {
        record = (struct containerRecord*) malloc(
					    sizeof(struct containerRecord));

		record->chunk_size = 0;
		record->delta_size = 0;
		record->base_size = baseSize;
		record->cid = id;

        g_hash_table_insert(container_utilization_monitor,
		    &record->cid, record);
    }
}

static gint g_record_cmp(struct containerRecord *a, struct containerRecord* b, gpointer user_data){
	return (a->chunk_size + a->delta_size + a->base_size) - (b->chunk_size + b->delta_size + b->base_size);
}

void close_har() {
	sds fname = sdsdup(destor.working_directory);
	fname = sdscat(fname, "recipes/bv");
	char s[20];
	sprintf(s, "%d", jcr.id);
	fname = sdscat(fname, s);
	fname = sdscat(fname, ".rewrite");

	sds s_DenseContainer = sdsdup(destor.working_directory);
	s_DenseContainer = sdscat(s_DenseContainer, "denseContainers/bv");
	s_DenseContainer = sdscat(s_DenseContainer, s);
	s_DenseContainer = sdscat(s_DenseContainer, ".dense");

	sds s_sparseContainer = sdsdup(destor.working_directory);
	s_sparseContainer = sdscat(s_sparseContainer, "denseContainers/bv");
	s_sparseContainer = sdscat(s_sparseContainer, s);
	s_sparseContainer = sdscat(s_sparseContainer, ".sparse");

	FILE* fp_rewrite = fopen(fname, "w");
	FILE* fp_dense =  fopen(s_DenseContainer, "w");
	FILE* fp_sparse =  fopen(s_sparseContainer, "w");

	if (!fp_rewrite || !fp_dense || !fp_sparse) {
		fprintf(stderr, "Can not open files");
		perror("The reason is");
		exit(1);
	}

	jcr.total_container_num_referred = g_hash_table_size(container_utilization_monitor);

	GSequence *seq_dense = g_sequence_new(NULL);
	GSequence *seq_rewrite = g_sequence_new(NULL);

	int64_t total_size = 0;
	int64_t rewrite_size = 0;

	/* collect sparse, dense, and rewrite containers */
    GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, container_utilization_monitor);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct containerRecord* cr = (struct containerRecord*)value;

		total_size += cr->chunk_size + cr->delta_size + cr->base_size;
		double reuse_ratio = (1.0 * (cr->chunk_size + cr->delta_size + cr->base_size))/CONTAINER_SIZE;

		if (reuse_ratio < destor.rewrite_har_utilization_threshold)
	    {
            rewrite_size += cr->chunk_size + cr->delta_size + cr->base_size;
            g_sequence_insert_sorted(seq_rewrite, cr, g_record_cmp, NULL);
		}

		if (reuse_ratio >= destor.sparse_utilization_threshold)
		{
			struct containerRecord* record = (struct containerRecord*) malloc(sizeof(struct containerRecord));
			record->chunk_size = cr->chunk_size;
			record->delta_size = cr->delta_size;
			record->base_size = cr->base_size;
			record->cid = cr->cid;
			g_sequence_insert_sorted(seq_dense, record, g_record_cmp, NULL);
		}
	}

	/*
	 * If the size of rewritten data is too large,
	 * we need to trim the sequence to control the rewrite ratio.
	 * We use sparse_size/total_size to estimate the rewrite ratio of next backup.
	 * However, the estimation is inaccurate (generally over-estimating), since:
	 * 	1. the sparse size is not an accurate indicator of utilization for next backup.
	 * 	2. self-references.
	 */
    while(destor.rewrite_har_rewrite_limit < 1
			&& rewrite_size*1.0/total_size > destor.rewrite_har_rewrite_limit){

		GSequenceIter* iter = g_sequence_iter_prev(g_sequence_get_end_iter(seq_rewrite));
		struct containerRecord* r = g_sequence_get(iter);
		VERBOSE("Trim sparse container %lld", r->cid);
		rewrite_size -= (r->chunk_size + r->delta_size + r->base_size);
		g_sequence_remove(iter);
	}

	GSequenceIter* rewrite_iter = g_sequence_get_begin_iter(seq_rewrite);
	while(rewrite_iter != g_sequence_get_end_iter(seq_rewrite)){
		struct containerRecord* cr = g_sequence_get(rewrite_iter);
		fprintf(fp_rewrite, "%lld %d\n", cr->cid, 0);
        rewrite_iter = g_sequence_iter_next(rewrite_iter);
	}
	fclose(fp_rewrite);

	double reuse_ratio;
	GSequenceIter* dense_iter = g_sequence_get_begin_iter(seq_dense);
	while(dense_iter != g_sequence_get_end_iter(seq_dense)){
		struct containerRecord* cr = g_sequence_get(dense_iter);
		reuse_ratio = (1.0 * (cr->chunk_size + cr->delta_size + cr->base_size))/CONTAINER_SIZE;

		if (reuse_ratio < destor.dense_utilization_threshold)
			fprintf(fp_sparse, "%lld %d\n", cr->cid, cr->chunk_size);
		else
			fprintf(fp_dense, "%lld %d\n", cr->cid, cr->chunk_size);
		
        dense_iter = g_sequence_iter_next(dense_iter);
	}
	fclose(fp_dense);
	fclose(fp_sparse);

    pthread_mutex_destroy(&mutex);

    g_sequence_free(seq_rewrite);
	g_sequence_free(seq_dense);

    sdsfree(fname);
	sdsfree(s_DenseContainer);
	sdsfree(s_sparseContainer);

	/* CMA: update the backup times in manifest */
	update_manifest(container_utilization_monitor);

    //g_hash_table_destroy(inherited_rewrite_containers);
	//g_hash_table_destroy(inherited_sparse_containers);
	//g_hash_table_destroy(inherited_dense_containers);
	
    //g_hash_table_destroy(container_utilization_monitor);

	jcr.container_num_after_backup = get_container_count();
	jcr.container_num_stored = jcr.container_num_after_backup - jcr.container_num_before_backup;
}

void har_check(struct chunk* c) {
	if (!CHECK_CHUNK(c, CHUNK_FILE_START)
        && !CHECK_CHUNK(c, CHUNK_FILE_END)
		&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START)
		&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
		&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {

        if (g_hash_table_contains(inherited_rewrite_containers, &c->id)) {
			assert(c->id != -1);
			SET_CHUNK(c, CHUNK_SPARSE);
		}

		if(!CHECK_CHUNK(c, CHUNK_SPARSE)
			&& !CHECK_CHUNK(c, CHUNK_BASE_FRAGMENTED)
			&& c->dup_with_delta) {

			assert(c->base_id != -1);
			if (g_hash_table_contains(inherited_rewrite_containers, &c->base_id))
				SET_CHUNK(c, CHUNK_BASE_FRAGMENTED);
		}
	}
}

void sparse_containers_update(containerid id) {
	containerid* p_cid = (containerid*)malloc(sizeof(containerid));
	*p_cid = id;
	g_hash_table_insert(inherited_sparse_containers, p_cid, NULL);
}

int rewrite_container_check(containerid id) {
	int res = 0;
    if (g_hash_table_contains(inherited_rewrite_containers, &id)) {
		res = 1;
	}
	return res;
}

int sparse_container_check(containerid id) {
	int res = 0;
    if (g_hash_table_contains(inherited_sparse_containers, &id)) {
		res = 1;
	}
	return res;
}

int dense_container_check(containerid id) {
	int res = 0;
    if (g_hash_table_contains(inherited_dense_containers, &id)) {
		res = 1;
	}
	return res;
}
