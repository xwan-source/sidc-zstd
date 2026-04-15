#ifndef HTABLE_H_
#define HTABLE_H_

#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#define foreach_htable(var, tbl) \
        for((*((void **)&(var))=(void *)((tbl)->first())); \
            (var); \
            (*((void **)&(var))=(void *)((tbl)->next())))

//typedef unsigned int u_int32_t;
//typedef unsigned long long int u_int64_t;

typedef unsigned int UINT32;
typedef unsigned long long int UINT64;

#define MAX_PATH 256

struct hlink {
   void *next;                        /* next hash item */
   unsigned char *key;                /* key this item */
   u_int64_t hash;                    /* hash for this key */
};

struct ChunkLink {
	char 	    szChunkHash[40];
	u_int32_t   nchunkSize;	
	struct hlink		psHashNextChunk; /*used for chunk temp hashtable*/
};

struct SegmentLink {
	char 		szRepchunkHash[40];
	//char 		szSegmentHash[HASH_SIZE];
	int 	nBlockID;
	void  		*psHashNextSeg;    /*this is used for hash table ?? */
};

/* 
* The purpose of "class htable" is to "look up" items with the "same" key.
* So it's a data structure for looking up. In other words, we can use some 
* other structures to replace it, such as the red-black tree, if we don't 
* care about the speed of looking up.
*/
struct htable {
   struct hlink **table;                     /* hash table */
   int hashlength;
   int loffset;                       /* link offset in item */
   u_int32_t num_items;                /* current number of items */
   u_int32_t max_items;                /* maximum items before growing */
   u_int32_t buckets;                  /* size of hash table */
   u_int64_t hash;                     /* temp storage */
   u_int32_t index;                    /* temp storage */
   u_int32_t mask;                     /* "remainder" mask */
   u_int32_t rshift;                   /* amount to shift down */
   struct hlink *walkptr;                    /* table walk pointer */
   u_int32_t walk_index;               /* table walk index */
   void (*hash_index)(struct htable*, unsigned char *key);        /* produce hash key,index */
   void (*grow_table)(struct htable*);                 /* grow the table */
   int (*insert)(struct htable*, unsigned char *key, void *item);
   void* (*lookup)(struct htable*, unsigned char *key);
   void* (*search)(struct htable*, unsigned char *key);
   void* (*first)(struct htable*);                     /* get first item in table */
   void* (*next)(struct htable*);                      /* get next item in table */
   void (*destroy)(struct htable*);
   void (*stats)(struct htable*);                      /* print stats about the table */
   u_int32_t (*size)(struct htable*);                   /* return size of table */
};

void init(struct htable** ht, int offset, int nhlen, int tsize);
void _hash_index(struct htable* ht, unsigned char *key);
u_int32_t _size(struct htable* ht);
void _stats(struct htable* ht);
void _grow_table(struct htable* ht);
int _insert(struct htable* ht, unsigned char *key, void *item);
void* _lookup(struct htable* ht, unsigned char *key);
void* _search(struct htable* ht, unsigned char *key);
void* _next(struct htable* ht);
void* _first(struct htable* ht);
void _destroy(struct htable* ht);
#endif