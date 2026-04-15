/*
 * An index_update() to a fingerprint will be called after the index_search() to the fingerprint.
 * However, due to the existence of rewriting algorithms,
 * there is no guarantee that the index_update() will be called immediately after the index_search().
 * Thus, we would better make them independent with each other.
 *
 * The input of index_search is nearly same as that of index_update() except the containerid field.
 */
#ifndef INDEX_H_
#define INDEX_H_

#include "../destor.h"

struct indexElem {
	containerid id;
	fingerprint fp;
    int32_t dup_with_delta;
	fingerprint base_fp;
    containerid base_id;
    int32_t base_size;
    int32_t delta_size;
	int32_t delta_compressed;
};

struct baseEntry {
    fingerprint fp;
    containerid id;
};

struct baseChunk {
    fingerprint fp;
    containerid id;
    int32_t size;
    unsigned char *data;
};

/*
 * The function is used to initialize memory structures of a fingerprint index.
 */
void init_index();
/*
 * Free memory structures and flush them into disks.
 */
void close_index();
/*
 * lookup fingerprints in a segment in index.
 */
int index_lookup(struct segment*);
void lookup_base(struct segment*);
/*
 * Insert/update fingerprints.
 */
void index_update(GHashTable *features, int64_t id);

void index_delete(fingerprint *fp, int64_t id);

extern GHashTable* (*sampling)(GQueue *chunks, int32_t chunk_num);
extern struct segment* (*segmenting)(struct chunk *c);

extern void init_segmenting_method();
extern void init_sampling_method();
extern gboolean g_container_equal(containerid*, containerid*);

extern GHashTable *base_sparse_chunks;

gboolean g_feature_equal(char* a, char* b);
guint g_feature_hash(char *feature);

void full_index_destroy();
void bloom_filter_flush();

void index_check_buffer(struct segment *s);
int index_update_buffer(struct segment *s);

void index_buffer_update_normal(struct chunk* ck);
void index_buffer_update_inverse(struct chunk* ck);

#endif
