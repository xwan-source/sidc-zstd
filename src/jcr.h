/*
 * jcr.h
 *
 *  Created on: Feb 15, 2012
 *      Author: fumin
 */

#ifndef Jcr_H_
#define Jcr_H_

#include "destor.h"
#include "recipe/recipestore.h"

#define JCR_STATUS_INIT 1
#define JCR_STATUS_RUNNING 2
#define JCR_STATUS_DONE 3

/* job control record */
struct {
	int32_t id;
	/*
	 * The path of backup or restore.
	 */
	sds path;

	int status;

	int32_t file_num;
	int64_t data_size;
	int64_t data_stored;
	int64_t chunk_num;
	int64_t normal_chunk_num;
	int64_t delta_chunk_num;
	int64_t zero_chunk_num;
	int64_t zero_chunk_size;
	int64_t rewritten_chunk_num;
	int64_t rewritten_size;

    int64_t rewritten_base_chunk_num;
    int64_t rewritten_base_chunk_size;

    int64_t container_num_before_backup;
    int64_t container_num_stored;
    int64_t container_num_after_backup;

    /*
     * container_num_now_stored used for determining
     * the container_id has been written to disk array
     * container_num_now_written used for determining
     * the container_id has been written to disk
     */
    int64_t container_num_now_written;
    int64_t container_num_now_stored;

    int32_t total_container_num_referred;
	int32_t sparse_container_num;
	int32_t inherited_sparse_num;

    int32_t base_chunk_referred;
    int64_t base_container_referenced;
	int64_t deduplication_container_referenced;

    int32_t container_num_of_first_backup;
    int32_t base_container_in_sparse;
    int32_t base_chunk_in_sparse_container;

	int32_t read_base_io;

	struct backupVersion* bv;

	double total_time;
	/*
	 * the time consuming of six dedup phase
	 */
	double read_time;
	double chunk_time;
	double hash_time;
	double dedup_time;
	double rewrite_time;
	double filter_time;
	double write_time;
    double sketch_time;
	double delta_compression_time;
	double local_compression_time;

	double container_utilization_computation_time;

	double read_recipe_time;
	double read_chunk_time;
	double write_chunk_time;

	int32_t read_container_num;
	
    int64_t number_of_chunks_have_deltas;
	int64_t unique_chunk_num;
	int32_t detected_duplicate_delta_num;
    int32_t duplicate_delta_num;
	int32_t duplicate_chunk_num;

    int64_t delta_compression_times;

    int64_t read_normal_chunks;
    int64_t read_base_chunks;
	
    int64_t hit_normal_chunks;
    int64_t hit_base_chunks;
    int64_t hit_sparse_base_cid;
    int64_t miss_sparse_base_cid;
	
    int unproper_delta_comp_num;
	
    int64_t total_size_for_similarity_detection;
	int64_t total_size_for_delta_compression;
	int64_t total_size_for_local_compression;

	int64_t chunk_size_before_local_compression;

	double mysql_lookup_time;
	int64_t mysql_lookup_times;

	int64_t disk_lookup_num;

	double time_to_read_base_container;

	int number_of_container_stored;

	int64_t base_container_cache_hits;
	int64_t base_container_cache_miss;

	int64_t sketch_replace_time;
    int64_t sketch_index_item_num;

	int64_t prefetch_index_times;
	double prefetch_index_time;
	int64_t prefetch_index_size;

	int64_t prefetch_base_times;
	double prefetch_base_time;
	int64_t prefetch_base_size;

	double sketch_lookup_time;
	double sketch_lookup_size;

	int64_t self_referenced_num;

	int64_t base_chunk_in_fcache;

	int64_t delta_compressed_chunk_num;
	int64_t inversed_compressed_chunk_num;

	int64_t prefetched_dense_base_container_num;
	int64_t prefetched_sparse_base_container_num;

	int64_t deduplicated_size;
    int64_t delta_compressed_size;
	int64_t local_compressed_size;

	int64_t inversed_delta_size;
	int64_t target_size_for_inversed_compression;

	/* 本地压缩统计 */
	int64_t local_compressed_chunk_num;      /* 被本地压缩的 chunk 数量 */
	int64_t local_uncompressed_chunk_num;    /* 未被压缩的 chunk 数量（压缩后反而更大或小于阈值） */
	int64_t local_skipped_chunk_num;         /* 跳过的 chunk 数量（差量块、重复块等） */
} jcr;

void init_jcr(char *path);
void init_backup_jcr(char *path);
void init_restore_jcr(int revision, char *path);

#endif /* Jcr_H_ */
