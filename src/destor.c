/*
 * server.c
 *
 *  Created on: May 24, 2012
 *      Author: fumin
 */

#include "destor.h"
#include "jcr.h"
#include "index/index.h"
#include "storage/containerstore.h"
#include "rewrite_phase.h"

extern void do_backup(char *path);
//extern void do_delete(int revision);
extern void do_restore(int revision, char *path);
extern void make_trace(char *raw_files);

extern int load_config();
extern void load_config_from_string(sds config);

/* : means argument is required.
 * :: means argument is required and no space.
 */
const char * const short_options = "sr::t::p::h";

struct option long_options[] = {
		{ "state", 0, NULL, 's' },
		{ "help", 0, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
};

void usage() {
	puts("GENERAL USAGE");
	puts("\tstart a backup job");
	puts("\t\tdestor /path/to/data -p\"a line in config file\"");

	puts("\tstart a restore job");
	puts("\t\tdestor -r<JOB_ID> /path/to/restore -p\"a line in config file\"");

	puts("\tprint state of destor");
	puts("\t\tdestor -s");

	puts("\tprint this");
	puts("\t\tdestor -h");

	puts("\tMake a trance");
	puts("\t\tdestor -t /path/to/data");

	puts("\tParameter");
	puts("\t\t-p\"a line in config file\"");
	exit(0);
}

void destor_log(int level, const char *fmt, ...) {
	va_list ap;
	char msg[DESTOR_MAX_LOGMSG_LEN];

	if ((level & 0xff) < destor.verbosity)
		return;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s\n", msg);
}

void check_simulation_level(int last_level, int current_level) {
	if ((last_level <= SIMULATION_RESTORE && current_level >= SIMULATION_APPEND)
			|| (last_level >= SIMULATION_APPEND
					&& current_level <= SIMULATION_RESTORE)) {
		fprintf(stderr, "FATAL ERROR: Conflicting simualtion level!\n");
		exit(1);
	}
}

void destor_start() {

	/* Init */
	destor.working_directory = sdsnew("/home/data/working/");
	destor.simulation_level = SIMULATION_NO;
	destor.verbosity = DESTOR_WARNING;

	destor.chunk_algorithm = CHUNK_RABIN;
	destor.chunk_max_size = 65536;
	destor.chunk_min_size = 2048;
	destor.chunk_avg_size = 8192;

	destor.restore_cache[0] = RESTORE_CACHE_LRU;
	destor.restore_cache[1] = 1024;
	destor.restore_opt_window_size = 1000000;

	destor.simulated_restore_cache_size = 32;
	destor.delta_compression_method = XDELTA;
	destor.mini_sketch_size = 2048;

	destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
	destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
	destor.index_specific = INDEX_SPECIFIC_NO;
	destor.index_key_value_store = INDEX_KEY_VALUE_HTABLE;
	destor.index_key_size = 20;
    destor.index_value_length = 1;

	destor.index_cache_size = 4096;

	destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
	destor.index_segment_algorithm[1] = 1024;
	destor.index_segment_min = 128;
	destor.index_segment_max = 10240;
	destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
	destor.index_sampling_method[1] = 1;
	destor.index_value_length = 1;
	destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
	destor.index_segment_selection_method[1] = 1;
	destor.index_segment_prefech = 0;

	destor.rewrite_algorithm[0] = REWRITE_NO;
	destor.rewrite_algorithm[1] = 1024;

	/* for History-Aware Rewriting (HAR) */
	destor.rewrite_enable_har = 0;
	destor.rewrite_har_utilization_threshold = 0.5;
    destor.sparse_chunk_number_threshold = 0.3;
	destor.rewrite_har_rewrite_limit = 0.05;
	destor.base_container_prefetching_percent = 0.8;

	destor.dense_utilization_threshold = 0.5;
	destor.sparse_utilization_threshold = 0.1;

	/* for Cache-Aware Filter */
	destor.rewrite_enable_cache_aware = 0;

    destor.enable_full_backup_after_threshold = 1;
    destor.full_backup_threshold_of_rewritten = 0;
    destor.enable_dedup_in_dense_full_backup = 0;

    destor.restore_speed_factor_to_fullbackup = 0;

    destor.remove_rewritten_sparse_chunks = 0;
    destor.base_container_num = 10;

	destor.har_include_self_reference = 0;

    destor.clear_history_info_for_full_backup = 1;
	/*
	 * Specify how many backups are retained.
	 * A negative value indicates all backups are retained.
	 */
	destor.backup_retention_time = -1;

    destor.index_start_point = 0;
    destor.sketch_method = SUPER_FEATURE;

    destor.super_feature_subregion_num = 9;
    destor.features_per_sf = 4;
	destor.hashing_method = SHA_1;
	destor.base_container_prefetch_num = 0;
	destor.number_of_item_for_caf = 32;
	
	destor.total_size_for_delta_compression = 0;
	destor.total_size_for_local_compression = 0;
	destor.deduplicated_size = 0;
	destor.delta_compressed_size = 0;
	destor.local_compressed_size = 0;

	destor.enable_locality_based_approach_for_resembling = 0;
	destor.rewrite_chunk_referencing_delta = 0;

	load_config();

	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "r"))) {

		fread(&destor.chunk_num, 8, 1, fp);

		fread(&destor.data_size, 8, 1, fp);
		fread(&destor.data_stored, 8, 1, fp);

		fread(&destor.total_size_for_delta_compression, 8, 1, fp);
		fread(&destor.total_size_for_local_compression, 8, 1, fp);
		fread(&destor.deduplicated_size, 8, 1, fp);
		fread(&destor.delta_compressed_size, 8, 1, fp);
		fread(&destor.local_compressed_size, 8, 1, fp);

		int last_retention_time;
		fread(&last_retention_time, 4, 1, fp);
		//assert(last_retention_time == destor.backup_retention_time);

		int last_level;
		fread(&last_level, 4, 1, fp);
		check_simulation_level(last_level, destor.simulation_level);

		fclose(fp);
	}
	else {
		destor.chunk_num = 0;
		destor.data_size = 0;
		destor.data_stored = 0;
		
		destor.total_size_for_delta_compression = 0;
		destor.total_size_for_local_compression = 0;
		destor.deduplicated_size = 0;
		destor.delta_compressed_size = 0;
		destor.local_compressed_size = 0;
	}

	sdsfree(stat_file);
}

void destor_shutdown() {
	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "w")) == 0) {
		destor_log(DESTOR_WARNING, "Fatal error, can not open destor.stat!");
		exit(1);
	}

	fwrite(&destor.chunk_num, 8, 1, fp);

	fwrite(&destor.data_size, 8, 1, fp);
	fwrite(&destor.data_stored, 8, 1, fp);

	fwrite(&destor.total_size_for_delta_compression, 8, 1, fp);
	fwrite(&destor.total_size_for_local_compression, 8, 1, fp);
	fwrite(&destor.deduplicated_size, 8, 1, fp);
	fwrite(&destor.delta_compressed_size, 8, 1, fp);
	fwrite(&destor.local_compressed_size, 8, 1, fp);

	fwrite(&destor.backup_retention_time, 4, 1, fp);
	fwrite(&destor.simulation_level, 4, 1, fp);

	fclose(fp);
	sdsfree(stat_file);
}

void destor_stat() {
	printf("=== destor stat ===\n");

	printf("the number of live containers: %d\n",
			destor.live_container_num);

	printf("the number of chunks: %ld\n", destor.chunk_num);

	printf("the size of data (B): %ld\n", destor.data_size);
	printf("the size of stored data (B): %ld\n", destor.data_stored);
	printf("the size of saved data (B): %ld\n",
			destor.data_size - destor.data_stored);

	printf("data reduction ratio: %.4f, %.4f\n",
			(destor.data_size - destor.data_stored)
					/ (double) destor.data_size,
			((double) destor.data_size) / (destor.data_stored));

	printf("the size of rewritten chunks (B): %ld\n",
			destor.rewritten_size);
	printf("rewrite ratio: %.4f\n",
			destor.rewritten_size / (double) destor.data_size);

	printf("deduplicated size: %ld, deduplicatation ratio: %.4f\n",
			destor.deduplicated_size, destor.data_size / (double) (destor.data_size - destor.deduplicated_size));

	printf("delta compressed size: %ld, delta compression efficiency: %.4f, delta compression ratio: %.4f\n",
						destor.delta_compressed_size, destor.delta_compressed_size / (double) destor.total_size_for_delta_compression,
						destor.total_size_for_delta_compression / (double) destor.delta_compressed_size);

	printf("local compressed size: %ld, local compression efficiency: %.4f, local compression ratio: %.4f\n",
						destor.local_compressed_size, 
						destor.local_compressed_size / (double) destor.total_size_for_local_compression,
						destor.total_size_for_local_compression / (double) destor.local_compressed_size);

	if (destor.simulation_level == SIMULATION_NO)
		printf("simulation level is %s\n", "NO");
	else if (destor.simulation_level == SIMULATION_RESTORE)
		printf("simulation level is %s\n", "RESTORE");
	else if (destor.simulation_level == SIMULATION_APPEND)
		printf("simulation level is %s\n", "APPEND");
	else if (destor.simulation_level == SIMULATION_ALL)
		printf("simulation level is %s\n", "ALL");
	else {
		printf("Invalid simulation level.\n");
	}

	printf("=== destor stat ===\n");
	exit(0);
}

int main(int argc, char **argv) {

	destor_start();

	int job = DESTOR_BACKUP;
	int revision = -1;

	int opt = 0;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {
		switch (opt) {
		case 'r':
			job = DESTOR_RESTORE;
			revision = atoi(optarg);
			break;
		case 's':
			destor_stat();
			break;
		case 't':
			job = DESTOR_MAKE_TRACE;
			break;
		case 'h':
			usage();
			break;
		case 'p': {
			sds param = sdsnew(optarg);
			load_config_from_string(param);
			break;
		}
		default:
			return 0;
		}
	}

	sds path;

	switch (job) {
	case DESTOR_BACKUP:

		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "backup job needs a protected path!\n");
			usage();
		}

		do_backup(path);

		/*
		 * The backup concludes.
		 * GC starts
		 * */
		if(destor.backup_retention_time >= 0
				&& jcr.id >= destor.backup_retention_time){
			NOTICE("GC is running!");
			do_delete(jcr.id - destor.backup_retention_time);
		}

		sdsfree(path);

		break;
	case DESTOR_RESTORE:
		if (revision < 0) {
			fprintf(stderr, "A job id is required!\n");
			usage();
		}
		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}

		do_restore(revision, path[0] == 0 ? 0 : path);

		sdsfree(path);
		break;
	case DESTOR_MAKE_TRACE: {
		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}

		make_trace(path);
		sdsfree(path);
		break;
	}
	default:
		fprintf(stderr, "Invalid job type!\n");
		usage();
	}
	destor_shutdown();

	return 0;
}

struct chunk* new_chunk(int32_t size) {
	struct chunk* ck = (struct chunk*) malloc(sizeof(struct chunk));

	ck->flag = CHUNK_UNIQUE;
	ck->id = TEMPORARY_ID;
	memset(&ck->fp, 0x0, sizeof(fingerprint));
    memset(&ck->base_fp, 0x0, sizeof(fingerprint));
	
	ck->size = size;
    ck->delta = NULL;
    ck->base_chunk = NULL;
	ck->local_compressed_contents = NULL;
    ck->dup_with_delta = 0;
    ck->base_id = -1;
    ck->base_size = 0;
    ck->delta_size = 0;
    ck->delta_compressed = 0;
	ck->inverse_encoded = 0;
	ck->size_after_local_compression = 0;
	ck->target_size_for_inversed_compression = 0;

	ck->stored_as_delta = -1;

    ck->sketches = (struct sketch*)malloc(sizeof(struct sketch));
    ck->sketches->sf1 = 0;
    ck->sketches->sf2 = 0;
    ck->sketches->sf3 = 0;

	if (size > 0) {
		ck->data = malloc(size);
	}
	else {
		ck->data = NULL;
	}

	return ck;
}

void free_chunk(struct chunk* ck) {

	if (ck->data) {
		free(ck->data);
		ck->data = NULL;
	}

	if(ck->delta) {
		free_delta(ck->delta);
		ck->delta = NULL;
	}

    if(ck->base_chunk) {
		free_basechunk(ck->base_chunk);
		ck->base_chunk = NULL;
    }

	if(ck->local_compressed_contents) {
		free(ck->local_compressed_contents);
		ck->local_compressed_contents = NULL;
	}

	if(ck->sketches) {
    	free(ck->sketches);
		ck->sketches = NULL;
	}
	
	free(ck);
}


struct delta* new_delta(int32_t size) {
	struct delta* d = (struct delta*) malloc(sizeof(struct delta));

	d->base_id = TEMPORARY_ID;
	d->data = malloc(size);
	d->size = size;
	d->inversedIsDuplicate = 0;
	memset(&d->base_fp, 0x0, sizeof(fingerprint));

	return d;
}

void free_delta(struct delta* d) {
	if(d->data) {
		free(d->data);
		d->data = NULL;
	}
	free(d);
}

void free_base_for_hashtable(gpointer data) {

    struct chunk* ck = (struct chunk*)data;
	if (ck->data) {
		free(ck->data);
		ck->data = NULL;
	}

	if(ck->delta)
		free_delta(ck->delta);

    free(ck->sketches);
	free(ck);
}

void free_basechunk(struct chunk* ck) {

	if (ck->data) {
		free(ck->data);
		ck->data = NULL;
	}

    if(ck->delta)
		free_delta(ck->delta);

	free(ck);
}

struct segment* new_segment() {
	struct segment * s = (struct segment*) malloc(sizeof(struct segment));
	s->id = TEMPORARY_ID;
	s->chunk_num = 0;
	s->chunks = g_queue_new();
	s->features = NULL;
	return s;
}

struct segment* new_segment_full(){
	struct segment* s = new_segment();
	s->features = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
	return s;
}

void free_segment(struct segment* s) {
	g_queue_free_full(s->chunks, free_chunk);

	if (s->features)
		g_hash_table_destroy(s->features);

	free(s);
}

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
	return !memcmp(fp1, fp2, sizeof(fingerprint));
}

gboolean g_superfeature_equal(uint64_t* sf1, uint64_t* sf2) {
	return (*sf1 == *sf2) ? 1 : 0;
}

gboolean g_container_equal(containerid* con1, containerid* con2) {
	return (*con1 == *con2) ? 1 : 0;
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
	return memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_chunk_cmp(struct chunk* a, struct chunk* b, gpointer user_data){
	return memcmp(&a->fp, b->fp, sizeof(fingerprint));
}
