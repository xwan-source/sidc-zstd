/*
 * destor.h
 *
 *  Created on: Nov 8, 2013
 *      Author: fumin
 */

#ifndef DESTOR_H_
#define DESTOR_H_

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/types.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include <openssl/sha.h>
#include <openssl/crypto.h>

#include <glib.h>
#include <getopt.h>

#include "utils/sds.h"

#define TIMER_DECLARE(n) struct timeval b##n,e##n
#define TIMER_BEGIN(n) gettimeofday(&b##n, NULL)
#define TIMER_END(n,t) gettimeofday(&e##n, NULL); \
    (t)+=e##n.tv_usec-b##n.tv_usec+1000000*(e##n.tv_sec-b##n.tv_sec)

#define DESTOR_CONFIGLINE_MAX 1024

#define DESTOR_BACKUP 1
#define DESTOR_RESTORE 2
#define DESTOR_MAKE_TRACE 3
#define DESTOR_DELETE 4

/* Log levels */
#define DESTOR_DEBUG 0
#define DESTOR_VERBOSE 1
#define DESTOR_NOTICE 2
#define DESTOR_WARNING 3
#define DESTOR_DEFAULT_VERBOSITY DESTOR_NOTICE
#define DESTOR_MAX_LOGMSG_LEN 1024

/* Simuation-level, ascending */
#define SIMULATION_NO 0
#define SIMULATION_RESTORE 1
#define SIMULATION_APPEND 2
#define SIMULATION_ALL 3

#define CHUNK_FIXED 0
#define CHUNK_RABIN 1
#define CHUNK_NORMALIZED_RABIN 2
#define CHUNK_AE 2

/*
 * A global fingerprint index is required.
 * A successful query returns a container id or a segment id for prefetching.
 * For those chunks failing to be deduplicated with prefetched containers or segments,
 * we further deduplicate them in the fingerprint index.
 * Thus none of stored chunks are duplicate.
 */
#define INDEX_CATEGORY_EXACT 0
/*
 * No global fingerprint index.
 * We only deduplicate chunks with prefetched containers or segments.
 * Without the global fingerprint index,
 * we require an in-memory feature index to determine which container or segment is prefetched.
 */
#define INDEX_CATEGORY_NEAR_EXACT 1
/*
 * Container is the unit of physical locality (locality).
 * Container is fixed-sized, and identified by an id.
 * In the category,
 * we select features in unit of container.
 */
#define INDEX_CATEGORY_PHYSICAL_LOCALITY 2
#define INDEX_CATEGORY_LOGICAL 2
/*
 * Segment is the unit of logical locality (similarity).
 * Segment can be either fixed-sized or variable-sized,
 * and identified by an id.
 * we select features in unit of segment.
 */
#define INDEX_CATEGORY_LOGICAL_LOCALITY 3
#define INDEX_CATEGORY_SIMILARITY 3

/*
 * Supported key-value store,
 * including hash table, MySQL.
 */
#define INDEX_KEY_VALUE_HTABLE 0
#define INDEX_KEY_VALUE_MYSQL 1
/*
 * Feature is used for prefetching segments (similarity) or containers (locality).
 * For example, when we find a duplicate chunk,
 * its feature is mapped to a container or a segment.
 * Pretching the container or segment can facilitate detecting following duplicate chunks.
 * ALL considers each fingerprint as a feature.
 * SAMPLE selects features via random sampling.
 * MIN selects minimal fingerprint(s) as feature.
 * UNIFORM selects a feature every n fingerprints.
 */
#define INDEX_SAMPLING_RANDOM 1
#define INDEX_SAMPLING_MIN 2
#define INDEX_SAMPLING_UNIFORM 3
#define INDEX_SAMPLING_OPTIMIZED_MIN 4

/*
 * Unlike container that is a physical unit,
 * segment is a logical unit.
 * We can divided a backup stream into segments via different methods.
 */
#define INDEX_SEGMENT_FIXED 0
#define INDEX_SEGMENT_CONTENT_DEFINED 1
#define INDEX_SEGMENT_FILE_DEFINED 2

/*
 * Many eligible segments may be found via looking up in feature index.
 * Further select champion segments in the eligible segments.
 * MIX indicates a mix of base and top.
 */
#define INDEX_SEGMENT_SELECT_BASE 0
#define INDEX_SEGMENT_SELECT_TOP 1
#define INDEX_SEGMENT_SELECT_MIX 2

/*
 * A specific fingerprint index,
 * similar with a combo.
 */
#define INDEX_SPECIFIC_NO 0
#define INDEX_SPECIFIC_DDFS 1
#define INDEX_SPECIFIC_EXTREME_BINNING 2
#define INDEX_SPECIFIC_SILO 3
#define INDEX_SPECIFIC_SPARSE 4
#define INDEX_SPECIFIC_SAMPLED 5
#define INDEX_SPECIFIC_BLOCK_LOCALITY_CACHING 6

#define RESTORE_CACHE_LRU 0

#define REWRITE_NO 0
#define REWRITE_CFL_SELECTIVE_DEDUPLICATION 1
#define REWRITE_CONTEXT_BASED 2
#define REWRITE_CAPPING 3

#define SUPER_FEATURE 0
#define FINESSE 1
#define ODESS 2

#define SHA_1 0
#define SHA_256 1

#define XDELTA 0
#define DDELTA 1
#define EDELTA 2

#define TEMPORARY_ID (-1L)

/* the buffer size for read phase */
#define DEFAULT_BLOCK_SIZE 1048576 //1MB

/* states of normal chunks. */
#define CHUNK_UNIQUE (0x0000)
#define CHUNK_DUPLICATE (0x0100)
#define CHUNK_SPARSE (0x0200)
#define CHUNK_BASE_FRAGMENTED (0x0400)
#define CHUNK_OUT_OF_ORDER (0x0800)

/* This flag will deny all rewriting, including fragmented chunks */
#define CHUNK_REWRITE_DENIED (0x1000)
#define CHUNK_BASE_NOT_IN_CACHE (0x2000)
#define CHUNK_SELF_REFERENCE (0x4000)

/* signal chunk */
#define CHUNK_FILE_START (0x0001)
#define CHUNK_FILE_END (0x0002)
#define CHUNK_SEGMENT_START (0x0004)
#define CHUNK_SEGMENT_END (0x0008)

#define SET_CHUNK(c, f) (c->flag |= f)
#define UNSET_CHUNK(c, f) (c->flag &= ~f)
#define CHECK_CHUNK(c, f) (c->flag & f)

/* Flags for restore */
#define CHUNK_WAIT 0
#define CHUNK_READY 1

struct destor {
	sds working_directory;
	int simulation_level;
	int verbosity;

	int chunk_algorithm;
	int chunk_max_size;
	int chunk_min_size;
	int chunk_avg_size;

	/* the cache type and size */
	int restore_cache[2];
	int restore_opt_window_size;
	int simulated_restore_cache_size;
	int delta_compression_method;
	int mini_sketch_size;
	int number_of_item_for_caf;

	/* Specify fingerprint index,
	 * exact or near-exact,
	 * physical logical locality */
	int index_category[2];
	/* optional */
	int index_specific;

	/* in number of containers, for DDFS/ChunkStash/Sampled Index. */
	int index_cache_size;
	int index_bloom_filter_size;

	/*
	 * [0] specifies the sampling method, and
	 * [1] specifies the sampling ratio.
	 */
	int index_sampling_method[2];
	/* Specify the key-value store. */
	int index_key_value_store;
	/* The max number of prefetching units a key can refer to */
	int index_value_length;
	/* the size of the key in byte */
	int index_key_size;

	/*
	 * [0] specifies the algorithm,
	 * and [1] specifies the average segment size.
	 */
	int index_segment_algorithm[2];
	int index_segment_min;
	int index_segment_max;

	int index_segment_selection_method[2];
	int index_segment_prefech;

	int rewrite_algorithm[2];
	int rewrite_enable_cfl_switch;
	/* for CFL-based selective deduplication */
	double rewrite_cfl_require;
	double rewrite_cfl_usage_threshold;
	/* for Context-Based Rewriting (CBR) */
	double rewrite_cbr_limit;
	double rewrite_cbr_minimal_utility;
	/* for capping */
	int rewrite_capping_level;

	/* for History-Aware Rewriting (HAR) */
	int rewrite_enable_har;
	int har_include_self_reference;
	double rewrite_har_utilization_threshold;
    double sparse_chunk_number_threshold;
	double rewrite_har_rewrite_limit;
	double base_container_prefetching_percent;

	/* for Cache-Aware Filter */
	int rewrite_enable_cache_aware;

    /* for specify if removing the rewritten sparse chunks */
    int remove_rewritten_sparse_chunks;

    int enable_full_backup_after_threshold;
    double full_backup_threshold_of_rewritten;
    int enable_dedup_in_dense_full_backup;
    double restore_speed_factor_to_fullbackup;

    int clear_history_info_for_full_backup;

	int enable_locality_based_approach_for_resembling;

    /* cache number of base container */
    int base_container_num;

	/* statistics of destor	 */
	int64_t chunk_num;
	int64_t stored_chunk_num;

	int64_t data_size;
	int64_t data_stored;

	int64_t zero_chunk_num;
	int64_t zero_chunk_size;

	int64_t rewritten_chunk_num;
	int64_t rewritten_size;

	int32_t index_memory_footprint;

	/* The number of live containers */
	int32_t live_container_num;

	int backup_retention_time;

    int index_start_point;

    int32_t sketch_method;
	int hashing_method;

    int super_feature_subregion_num;

    int features_per_sf;
	int64_t base_container_prefetch_num;
	double dense_utilization_threshold;
	double sparse_utilization_threshold;

	int rewrite_chunk_referencing_delta;

	int64_t total_size_for_delta_compression;
	int64_t total_size_for_local_compression;
	int64_t deduplicated_size;
	int64_t delta_compressed_size;
	int64_t local_compressed_size;

	int64_t self_referenced_num;
} destor;

typedef unsigned char fingerprint[20];
typedef unsigned char sufeature[16];
typedef int64_t containerid; //container id
typedef int64_t segmentid;
//typedef unsigned long long int UINT64;

struct sketch {
    uint64_t sf1;
    uint64_t sf2;
    uint64_t sf3;
};

struct delta {
	int32_t size;       			/* the size of delta */
    int32_t base_size;  			/* the size of base chunk */
	int32_t original_size;			/* the size of the chunk before delta compression */
	int32_t inversedIsDuplicate;  	/* indicate whether the inversted delta is duplicate */
	unsigned char *data;			/* delta content */
	containerid base_id;
	fingerprint base_fp;
	fingerprint fp;					/* fingerprint of the chunk before delta compression */
};

struct chunk {
	int32_t size;				/* size of the chunk */
	int flag;					/* indicat whether it is a delta (=1) */
	containerid id;				/* container id of the chunk */
	fingerprint fp;				/* fingerprint of of the chunk */
	unsigned char* data;		/* chunk contents */

	struct delta* delta;		/* delta contents */

    int dup_with_delta;     	/* indicat whether the chunk refers to a prevoiusly-stored delta (=1) */
    int delta_compressed;		/* indicat whether the chunk is delta-compressed (=1) */
    containerid base_id;		/* container id of the base chunk */
    fingerprint base_fp;		/* fingerprint of the base chunk */
    struct sketch* sketches;	/* SFs of the chunk */
    struct chunk* base_chunk;	/* base chunk */
    int32_t delta_size;			/* size of the delta */
    int32_t base_size;			/* size of the base */
	int32_t inverse_encoded;	/* if the chunk has been inversted encoded */

	unsigned char* local_compressed_contents;
	int32_t size_after_local_compression;

	int32_t target_size_for_inversed_compression;

	int32_t stored_as_delta;
};

/* struct segment only makes sense for index. */
struct segment {
	segmentid id;
	/* The actual number because there are signal chunks. */
	int32_t chunk_num;
	GQueue *chunks;
	GHashTable* features;
};

struct chunk* new_chunk(int32_t size);
void free_chunk(struct chunk*);
struct delta* new_delta(int32_t size);
void free_delta(struct delta* d);
void free_basechunk(struct chunk*);
void free_base_for_hashtable(gpointer data);

struct segment* new_segment();
struct segment* new_segment_full();
void free_segment(struct segment* s);

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2);
gboolean g_superfeature_equal(uint64_t* sf1, uint64_t* sf2);
gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data);
gint g_chunk_cmp(struct chunk* a, struct chunk* b, gpointer user_data);

void hash2code(unsigned char hash[20], char code[40]);

#define DEBUG(fmt, arg...) destor_log(DESTOR_DEBUG, fmt, ##arg);
#define VERBOSE(fmt, arg...) destor_log(DESTOR_VERBOSE, fmt, ##arg);
#define NOTICE(fmt, arg...) destor_log(DESTOR_NOTICE, fmt, ##arg);
#define WARNING(fmt, arg...) destor_log(DESTOR_WARNING, fmt, ##arg);

void destor_log(int level, const char *fmt, ...);

#endif /* DESTOR_H_ */
