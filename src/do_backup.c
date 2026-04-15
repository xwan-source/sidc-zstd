#include "destor.h"
#include "jcr.h"
#include "utils/sync_queue.h"
#include "index/index.h"
#include "backup.h"

/* defined in index.c */
extern struct {
	/* Requests to the key-value store */
	int lookup_requests;
	int update_requests;
	int lookup_requests_for_unique;
	/* Overheads of prefetching module */
	int read_prefetching_units;
}index_overhead;

extern void restore_aware_close();
extern void close_har();

void do_backup(char *path) {

	init_recipe_store();
	init_container_store();
	init_index();

	init_backup_jcr(path);

    if (jcr.id == 0)
        jcr.container_num_before_backup = 0;
    else
        jcr.container_num_before_backup = get_container_count();

	puts("==== backup begin ====");

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	start_read_phase();
	start_chunk_phase();
	start_hash_phase();

	start_dedup_phase();
	start_rewrite_phase();
    start_resemblance_phase();
    start_matching_phase();
	start_delta_phase();
	start_local_compression_phase();
	start_filter_phase();

	 do{
        usleep(500);
        time_t now = time(NULL);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);

	stop_read_phase();
	stop_chunk_phase();
	stop_hash_phase();

	stop_dedup_phase();
	stop_rewrite_phase();
    stop_resemblance_phase();
    stop_matching_phase();
	stop_delta_phase();
	stop_local_compression_phase();
	stop_filter_phase();

    close_har();
    restore_aware_close();
	close_index();
	close_container_store();
	close_recipe_store();

	TIMER_END(1, jcr.total_time);

	update_backup_version(jcr.bv);

	free_backup_version(jcr.bv);

    //puts("\n");
	puts("==== backup end ====");

	printf("job id: %d\n", jcr.id);
	printf("backup path: %s\n", jcr.path);

	printf("number of files: %d\n", jcr.file_num);
	printf("number of chunks: %d (%ld bytes on average)\n", jcr.chunk_num,
			jcr.data_size / jcr.chunk_num);

	printf("stored data size(B): %ld\n",
			jcr.data_stored);
	printf("data reduction ratio after deduplication: %.4f, %.4f\n",
			jcr.data_size != 0 ?
					(jcr.deduplicated_size)
							/ (double) (jcr.data_size) :
					0,
			jcr.data_size
					/ (double) (jcr.data_size - jcr.deduplicated_size));
	printf("local compression stats: compressed_chunks=%lld, uncompressed_chunks=%lld, skipped_chunks=%lld\n",
			jcr.local_compressed_chunk_num, jcr.local_uncompressed_chunk_num, jcr.local_skipped_chunk_num);
	printf("local compression size: before=%lld, saved=%lld\n",
			jcr.chunk_size_before_local_compression, jcr.local_compressed_size);
	printf("data reduction ratio after local compression: %.4f, %.4f\n",
			jcr.data_size != 0 ?
					(jcr.deduplicated_size + jcr.local_compressed_size)
							/ (double) (jcr.data_size) :
					0,
			jcr.data_size - jcr.deduplicated_size - jcr.local_compressed_size > 0 ?
					jcr.data_size
							/ (double) (jcr.data_size - jcr.deduplicated_size - jcr.local_compressed_size) :
					0);
	printf("delta compression stats: chunks=%lld, before=%lld, saved=%lld\n",
			jcr.delta_compressed_chunk_num, jcr.total_size_for_delta_compression, jcr.delta_compressed_size);
	printf("data reduction ratio after delta compression: %.4f, %.4f\n",
			jcr.data_size != 0 ?
					(jcr.deduplicated_size + jcr.local_compressed_size + jcr.delta_compressed_size)
							/ (double) (jcr.data_size) :
					0,
			jcr.data_size - jcr.deduplicated_size - jcr.local_compressed_size - jcr.delta_compressed_size > 0 ?
					jcr.data_size
							/ (double) (jcr.data_size - jcr.deduplicated_size - jcr.local_compressed_size - jcr.delta_compressed_size) :
					0);

    printf("unproper delta compression number: %d\n", jcr.unproper_delta_comp_num);
	printf("number of rewritten chunks: %d\n", jcr.rewritten_chunk_num);

	destor.data_size += jcr.data_size;
	destor.data_stored += jcr.data_stored;

	destor.chunk_num += jcr.chunk_num;
	destor.stored_chunk_num += jcr.normal_chunk_num + jcr.delta_chunk_num;
	destor.zero_chunk_num += jcr.zero_chunk_num;
	destor.zero_chunk_size += jcr.zero_chunk_size;
	destor.rewritten_chunk_num += jcr.rewritten_chunk_num;
	destor.rewritten_size += jcr.rewritten_size;

	destor.total_size_for_delta_compression += jcr.total_size_for_delta_compression;
	destor.total_size_for_local_compression += jcr.total_size_for_local_compression;
	destor.deduplicated_size += jcr.deduplicated_size;
	destor.delta_compressed_size += jcr.delta_compressed_size;
	destor.local_compressed_size += jcr.local_compressed_size;
	
	destor.self_referenced_num += jcr.self_referenced_num;

	printf("read_time : %.3fs, %.2fMB/s\n", jcr.read_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_time / 1024 / 1024);
	printf("chunk_time : %.3fs, %.2fMB/s\n", jcr.chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.chunk_time / 1024 / 1024);
	printf("hash_time : %.3fs, %.2fMB/s\n", jcr.hash_time / 1000000,
			jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024);

	printf("dedup_time : %.3fs, %.2fMB/s\n",
			jcr.dedup_time / 1000000,
			jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024);

	printf("sketch_time : %.3fs, %.2fMB/s\n", jcr.sketch_time / 1000000,
			jcr.total_size_for_similarity_detection * 1000000 / jcr.sketch_time / 1024 / 1024);

	printf("delta_encoding_time : %.3fs, %.2fMB/s\n", jcr.delta_compression_time / 1000000,
			jcr.total_size_for_delta_compression * 1000000 / jcr.delta_compression_time / 1024 / 1024);

	printf("rewrite_time : %.3fs, %.2fMB/s\n", jcr.rewrite_time / 1000000,
			jcr.data_size * 1000000 / jcr.rewrite_time / 1024 / 1024);

	/*
	printf("sketch_lookup_time : %.3fs, %.2fMB/s, %.2fMB/s, index prefetch: %lld, container prefetch: %lld\n", jcr.sketch_lookup_time / 1000000,
			jcr.sketch_lookup_size * 1000000 / jcr.sketch_lookup_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.sketch_lookup_time / 1024 / 1024,
			jcr.prefetch_index_times, jcr.prefetch_base_times);
	*/

	/*
	printf("sketch_replace_time : %.3fs, %.2fMB/s\n", jcr.sketch_replace_time / 1000000,
			jcr.sketch_replace_time == 0 ? 0 : jcr.data_size * 1000000 / jcr.sketch_replace_time / 1024 / 1024);
	*/
	
	printf("filter_time : %.3fs, %.2fMB/s\n",
			jcr.filter_time / 1000000,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024);

	printf("write_time : %.3fs, %.2fMB/s\n", jcr.write_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024);

	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("total size(B): %ld\n", jcr.data_size);
	printf("number of detected duplicate delta chunks: %d\n", jcr.detected_duplicate_delta_num);
	printf("number of base chunks in fp-cache for chunks dup with delta: %ld\n", jcr.base_chunk_in_fcache);
    printf("throughput(MB/s): %.2f\n",
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	printf("number of rewritten chunks: %d\n", jcr.rewritten_chunk_num);
	printf("number of delta compression chunks: %d\n", jcr.delta_compressed_chunk_num);
	printf("number of inversed compression chunks: %d\n", jcr.inversed_compressed_chunk_num);
	
    printf("\n====================================================================================\n");
    printf("====================================================================================\n");

	char logfile[] = "backup.log";
	FILE *fp = fopen(logfile, "a");
	/*
	 * job id,
	 * the size of backup
	 * accumulative consumed capacity,
	 * deduplication rate,
	 * rewritten rate,
	 * total container number referred,
	 * sparse container number,
	 * inherited container number,
	 * 4 * index overhead (4 * int)
	 * throughput,
	 */
	fprintf(fp, "%d %ld %ld %.4f %.4f %d %d %d %d %d %d %d %.2f\n",
			jcr.id,
			jcr.data_size,
			destor.data_stored,
			jcr.data_size != 0 ?
					(jcr.data_size - jcr.data_stored)/(double) (jcr.data_size)
					: 0,
			jcr.data_size != 0 ? (double) (jcr.data_size - jcr.data_stored)
													/ (double) (jcr.data_size) : 0,
			jcr.total_container_num_referred,
			jcr.sparse_container_num,
			jcr.inherited_sparse_num,
			index_overhead.lookup_requests,
			index_overhead.lookup_requests_for_unique,
			index_overhead.update_requests,
			index_overhead.read_prefetching_units,
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);

    FILE *excel_backup = fopen("excel_backup.txt", "a");
    fprintf(excel_backup, "%d\t%d\t%ld\t%d\n", jcr.id + 1,
            jcr.chunk_num,
            jcr.data_size / jcr.chunk_num,
            jcr.container_num_after_backup);
    fclose(excel_backup);

    FILE *excel_dedup = fopen("excel_dedup.txt", "a");
    fprintf(excel_dedup, "%d\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", jcr.id + 1,
        jcr.data_size != 0 ? (jcr.data_size - (jcr.data_stored + jcr.delta_compressed_size + jcr.local_compressed_size + jcr.inversed_delta_size - jcr.rewritten_size))
							/ (double) (jcr.data_size) : 0,
			jcr.data_size / (double) (jcr.data_stored + jcr.delta_compressed_size + jcr.local_compressed_size + jcr.inversed_delta_size - jcr.rewritten_size),

		jcr.data_size != 0 ? (jcr.data_size - (jcr.data_stored + jcr.delta_compressed_size + jcr.inversed_delta_size - jcr.rewritten_size))
							/ (double) (jcr.data_size) : 0,
			jcr.data_size / (double) (jcr.data_stored + jcr.delta_compressed_size + jcr.inversed_delta_size - jcr.rewritten_size),
	
		jcr.data_size != 0 ? (jcr.data_size - (jcr.data_stored - jcr.target_size_for_inversed_compression + jcr.inversed_delta_size - jcr.rewritten_size))
							/ (double) (jcr.data_size) : 0,
			jcr.data_size / (double) (jcr.data_stored - jcr.target_size_for_inversed_compression + jcr.inversed_delta_size - jcr.rewritten_size));
    fclose(excel_dedup);
	/*
    FILE *excel_throughput = fopen("excel_throughput.txt", "a");
    fprintf(excel_throughput, "%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", jcr.id + 1,
			jcr.data_size * 1000000 / jcr.chunk_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024,
			jcr.total_size_for_similarity_detection * 1000000 / jcr.sketch_time / 1024 / 1024,
			jcr.total_size_for_delta_compression * 1000000 / jcr.delta_compression_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.rewrite_time / 1024 / 1024,
			jcr.data_size * 1000000 / (jcr.prefetch_base_time + jcr.prefetch_index_time ) / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.sketch_lookup_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024,
        (double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
    fclose(excel_throughput);
	*/

    FILE *excel_delta_efficiency = fopen("excel_delta_efficiency.txt", "a");
    fprintf(excel_delta_efficiency, "%d\t%ld\t%ld\t%.4f\n", jcr.id + 1,
						jcr.delta_compressed_size, 
						jcr.total_size_for_delta_compression,
						1.0*jcr.delta_compressed_size/jcr.total_size_for_delta_compression);
    fclose(excel_delta_efficiency);
	
	FILE *excel_seconds_each_phase = fopen("excel_seconds_each_phase.txt", "a");
    fprintf(excel_seconds_each_phase, "%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
			jcr.id + 1,
			jcr.read_time  / 1000000,
			jcr.chunk_time  / 1000000,
			jcr.hash_time  / 1000000,
			jcr.dedup_time  / 1000000,
			jcr.sketch_time  / 1000000,
			jcr.delta_compression_time  / 1000000,
			jcr.rewrite_time  / 1000000,
			jcr.filter_time / 1000000,
			jcr.write_time / 1000000,
        	jcr.total_time / 1000000);
    fclose(excel_seconds_each_phase);

	FILE *excel_seconds_sketch = fopen("excel_seconds_sketch.txt", "a");
    fprintf(excel_seconds_sketch, "%d\t%.3f\t%.3f\t%.3f\t%.3f\n",
			jcr.id + 1,
			jcr.prefetch_base_time / 1000000,
			jcr.prefetch_index_time / 1000000,
			jcr.sketch_lookup_time / 1000000,
			jcr.sketch_replace_time / 1000000);
    fclose(excel_seconds_sketch);

	FILE *excel_rewritten_statis = fopen("excel_rewritten_statis.txt", "a");
    fprintf(excel_rewritten_statis, "%d\t%lld\t%lld\t%lld\n", jcr.id + 1,
						jcr.rewritten_chunk_num,
						jcr.unique_chunk_num,
						jcr.chunk_num);
    fclose(excel_rewritten_statis);

	FILE *excel_prefetch_status = fopen("excel_prefetch_status.txt", "a");
    fprintf(excel_prefetch_status, "%d\t%.3f\t%ld\t%.2f\t%.3f\t%ld\t%.2f\t%ld\t%ld\n", jcr.id + 1,
        jcr.prefetch_index_time / 1000000, 
        jcr.prefetch_index_times,
        jcr.prefetch_index_size * 1000000 / jcr.prefetch_index_time / 1024 / 1024,
        jcr.prefetch_base_time / 1000000, 
        jcr.prefetch_base_times,
        jcr.prefetch_base_size * 1000000 / jcr.prefetch_base_time / 1024 / 1024,
        jcr.prefetch_index_times,
        jcr.prefetch_base_times);
	fclose(excel_prefetch_status);

	FILE *excel_full_prefetch_status = fopen("excel_full_prefetch_status.txt", "a");
    fprintf(excel_full_prefetch_status, "%d\t%.3f\t%ld\t%.2f\t%.3f\t%ld\t%.2f\n", jcr.id + 1,
        jcr.prefetch_index_time / 1000000, jcr.prefetch_index_times,
        jcr.data_size * 1000000 / jcr.prefetch_index_time / 1024 / 1024,
        jcr.prefetch_base_time / 1000000, jcr.prefetch_base_times,
        jcr.data_size * 1000000 / jcr.prefetch_base_time / 1024 / 1024,
        jcr.mysql_lookup_time / 1000000, jcr.data_size * 1000000 / jcr.mysql_lookup_time / 1024 / 1024);
	fclose(excel_full_prefetch_status);

	FILE *excel_mysql_lookup = fopen("excel_mysql_lookup.txt", "a");
    fprintf(excel_mysql_lookup, "%d\t%.3f\t%ld\t%.2f\n", jcr.id + 1,
        jcr.mysql_lookup_time / 1000000,
        jcr.mysql_lookup_times,
        jcr.data_size * 1000000 / jcr.mysql_lookup_time / 1024 / 1024);
	fclose(excel_mysql_lookup);

	FILE *excel_base_container_cache_statistics = fopen("excel_base_container_cache_statistics.txt", "a");
    fprintf(excel_base_container_cache_statistics, "%d\t%ld\t%ld\t%.2f\n", jcr.id + 1,
        jcr.base_container_cache_hits, 
        jcr.base_container_cache_miss,
        1.0*jcr.base_container_cache_hits/(jcr.base_container_cache_hits+jcr.base_container_cache_miss));
	fclose(excel_base_container_cache_statistics);

	FILE *excel_container_and_prefetching_statis = fopen("excel_container_and_prefetching_statis.txt", "a");
    fprintf(excel_container_and_prefetching_statis, "%d\t%ld\t%ld\t%ld\t%ld\t%ld\n", jcr.id + 1,
		jcr.deduplication_container_referenced,
		jcr.base_container_referenced,
		jcr.container_num_before_backup,
		jcr.container_num_stored,
		jcr.container_num_after_backup);
	fclose(excel_container_and_prefetching_statis);

	FILE *excel_disk_access_statis = fopen("excel_disk_access_statis.txt", "a");
    fprintf(excel_disk_access_statis, "%d\t%ld\t%ld\t%ld\t%ld\t%ld\n", jcr.id + 1,
			jcr.mysql_lookup_times,
			jcr.prefetch_index_times,
			jcr.prefetch_base_times,
            jcr.container_num_stored,
            jcr.container_num_after_backup);
    fclose(excel_disk_access_statis);

	FILE *excel_chunks_statistics = fopen("excel_chunks_statistics.txt", "a");
    fprintf(excel_chunks_statistics, "%d\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", jcr.id + 1,
								jcr.chunk_num,
								jcr.unique_chunk_num,
								jcr.duplicate_chunk_num,
								jcr.duplicate_delta_num,
                                jcr.rewritten_chunk_num,
                                jcr.delta_compressed_chunk_num,
                                jcr.inversed_compressed_chunk_num);
    fclose(excel_chunks_statistics);

	FILE *excel_base_container_prefetch_num = fopen("excel_base_container_prefetch_num.txt", "a");
    fprintf(excel_base_container_prefetch_num, "%d\t%lld\t%lld\t%lld\t%lld\n", jcr.id + 1,
                                destor.base_container_prefetch_num,
                                jcr.prefetched_dense_base_container_num,
                                jcr.prefetched_sparse_base_container_num,
                                jcr.prefetch_index_times);
    fclose(excel_base_container_prefetch_num);

	FILE *excel_data_size_statistics = fopen("excel_data_size_statistics.txt", "a");
    fprintf(excel_data_size_statistics, "%d\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", jcr.id + 1,
			jcr.data_size,
			jcr.data_stored,
			jcr.deduplicated_size,
			jcr.rewritten_size,
            jcr.delta_compressed_size,
            jcr.local_compressed_size);
    fclose(excel_data_size_statistics);

	/* 本地压缩统计 */
	FILE *excel_local_compression = fopen("excel_local_compression.txt", "a");
    fprintf(excel_local_compression, "%d\t%lld\t%lld\t%lld\t%lld\t%lld\t%.2f\t%.2f\n", jcr.id + 1,
			jcr.local_compressed_chunk_num,
			jcr.local_uncompressed_chunk_num,
			jcr.local_skipped_chunk_num,
			jcr.chunk_size_before_local_compression,
			jcr.local_compressed_size,
			jcr.chunk_size_before_local_compression > 0 ?
				(double)jcr.local_compressed_chunk_num / (jcr.local_compressed_chunk_num + jcr.local_uncompressed_chunk_num) * 100 : 0,
			jcr.chunk_size_before_local_compression > 0 ?
				(double)jcr.local_compressed_size / jcr.chunk_size_before_local_compression * 100 : 0);
    fclose(excel_local_compression);

	FILE *excel_throughput = fopen("excel_throughput.txt", "a");
    fprintf(excel_throughput, "%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", jcr.id + 1,
			jcr.data_size * 1000000 / jcr.chunk_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024,
			jcr.total_size_for_similarity_detection * 1000000 / jcr.sketch_time / 1024 / 1024,
			jcr.total_size_for_delta_compression * 1000000 / jcr.delta_compression_time / 1024 / 1024,
			jcr.chunk_size_before_local_compression  * 1000000/ jcr.local_compression_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.rewrite_time / 1024 / 1024,
			jcr.data_size * 1000000 / (jcr.prefetch_base_time + jcr.prefetch_index_time ) / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.sketch_lookup_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024,
			jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024,
        (double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
    fclose(excel_throughput);

	FILE *excel_read_fp_io = fopen("excel_read_fp_io.txt","a");
	fprintf(excel_read_fp_io,"%d\t%d\n",jcr.id,index_overhead.read_prefetching_units);
	fclose(excel_read_fp_io);

	FILE *excel_read_base_io = fopen("excel_read_base_io.txt","a");
	fprintf(excel_read_fp_io,"%d\t%d\n",jcr.id,jcr.read_base_io);
	fclose(excel_read_base_io);

}
