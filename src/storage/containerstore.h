/*
 * containerstore.h
 *
 *  Created on: Nov 11, 2013
 *      Author: fumin
 */

#ifndef CONTAINERSTORE_H_
#define CONTAINERSTORE_H_

#include "../destor.h"

#define CONTAINER_SIZE (4194304ll) //4MB

/* 
 * CONTAINER_HEAD includes three elenents: containerid(8B), chunk_num(4B), 
 * buffer_num(4B), and data_size(4B) 
 */
#define CONTAINER_HEAD 16

/*
 * a CONTAINER_META_ENTRY consists of 20 bytes fp, 4 bytes offset, 4 bytes length,
 * 20 bytes base_fp, 4 bytes base_size, 8 bytes sf1, 8 bytes sf2, 8 bytes sf3,
 * 1 byte flag, and 4 bytes chunk_len (original size for local compression)
 */
#define CONTAINER_META_ENTRY 81

struct containerMeta {
	containerid id;
	int32_t data_size;
	int32_t chunk_num;

	/* Map fingerprints to chunk offsets. */
	GHashTable *map;
};

struct metaEntry {
	int32_t chunk_len;  	// offset of the compressed buffer in the container
	int32_t offset;            			// segment length
	int32_t data_len;						// chunk length
	/*
	 * the flag indicates whether it is a delta
	 * 0 -> base chunk
	 * 1 -> delta chunk
	 * */
	char flag;
	fingerprint fp;
	fingerprint base_fp;
	containerid base_id;

    int32_t delta_size;
    int32_t base_size;

	uint64_t sf1;
    uint64_t sf2;
    uint64_t sf3;
};

/*
 * Container is fixed-sized structure (4MB).
 * It consists of two portions: metadata and data portions.
 * All chunks are sequentially written to the data portions,
 * and are indexed by the metadata portion.
 *
 * The metadata portion consists of two parts:
 * 1) the head describing the container
 * 2) the meta entries
 */
struct container {
	struct containerMeta meta;
	unsigned char *data;
};

void init_container_store();
void close_container_store();

struct container* create_container();

containerid get_container_id(struct container* c);
void write_container_async(struct container*);
void write_container(struct container*);

struct containerMeta* load_container_meta_by_id(containerid id);
struct container* load_container_by_id(containerid);
struct container* get_container_in_write_buffer(containerid id);
struct container* load_container_for_meta_and_chunk(containerid id);

struct containerMeta* container_meta_duplicate(struct container *c);
struct container* container_duplicate(struct container*);
struct container* retrieve_container_in_write_buffer(containerid id);

struct metaEntry* get_meta_entry_in_container_meta(struct containerMeta* cm, fingerprint *fp);
int check_container_in_list(struct container *con, int64_t cid);
struct chunk* get_chunk_in_container(struct container*, fingerprint*);
struct chunk* get_base_chunk_in_container(struct container*, fingerprint*);

inline int calculate_meta_size(int chunk_num);
int container_overflow(struct container* c, struct chunk* ck);

int add_chunk_to_container(struct container* c, struct chunk* ck);

void free_container_meta(struct containerMeta* cm);
void free_container(struct container* c);

int container_empty(struct container* c);

int lookup_fingerprint_in_container_meta(struct containerMeta* cm,	fingerprint *fp);
int lookup_fingerprint_in_container(struct container* c, fingerprint *fp);

int delta_lookup_fingerprint_in_container(struct container* c, struct chunk* ck);

gint g_container_cmp_desc(struct container*, struct container*, gpointer);
gint g_container_meta_cmp_desc(struct containerMeta*, struct containerMeta*, gpointer);


int container_check_id(struct container*, containerid*);
int container_meta_check_id(struct containerMeta*, containerid*);
void container_meta_foreach(struct containerMeta* cm, void (*func)(fingerprint*, void*), void* data);

int64_t get_container_count();

#endif /* CONTAINERSTORE_H_ */
