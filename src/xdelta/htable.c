#include "htable.h"
/*
 * Take each hash link and walk down the chain of items
 *  that hash there counting them (i.e. the hits), 
 *  then report that number.
 *  Obiously, the more hits in a chain, the more time
 *  it takes to reference them. Empty chains are not so
 *  hot either -- as it means unused or wasted space.
 */
#define MAX_COUNT 20
 
#define HASH_SIZE 40 
#define UINT32_SIZE 4 

void _hash_index(struct htable* ht, unsigned char *key) { 
  
   if(ht->hashlength == 8 && key)
   {
   		memcpy((char*)&(ht->hash), key, ht->hashlength);
		ht->index = ht->hash & ht->mask;
		return;
   }
   if(key == NULL)
   {
   		printf("%s,%d:Insert key==NULL\n",__FILE__,__LINE__);
		ht->index = 0;
   		return ;
   }
   int i;
   for (i = ht->hashlength - 1; ht->hash < ht->buckets && i > 0; i--) {
      ht->hash += (ht->hash *16) + (key[i]<'9'?key[i]-'0':10+key[i]-'A');
   }
   ht->index = ht->hash & ht->mask;

   return;
}

/* @tsize: num of max_items before growing ,so
* tsize=max_items=buckets * 4
*
* @nhlen: hashlength in bytes
*/
void init(struct htable** ht, int offset, int nhlen, int tsize)
{
   int pwr;
   tsize >>= 2; //because: max_items = buckets * 4;
   for (pwr=0; tsize; pwr++) {
      tsize >>= 1;
   }
   (*ht)->loffset = offset;
   (*ht)->mask = ~((~0)<<pwr);               /* 3 bits => table size = 8 */
   (*ht)->rshift = 30 - pwr;                 /* start using bits 28, 29, 30 */
   (*ht)->num_items = 0;                     /* number of entries in table */
   (*ht)->buckets = 1<<pwr;                  /* hash table size -- power of two */
   (*ht)->max_items = (*ht)->buckets * 4;           /* allow average 4 entries per chain */
   (*ht)->table = (struct hlink **)malloc((*ht)->buckets * sizeof(struct hlink *));
   memset((*ht)->table, 0, (*ht)->buckets * sizeof(struct hlink *));
   (*ht)->walkptr = NULL;
   (*ht)->walk_index = 0;
   (*ht)->hashlength = nhlen;

   (*ht)->hash_index = _hash_index;
   (*ht)->grow_table = _grow_table;
   (*ht)->insert = _insert;
   (*ht)->lookup = _lookup;
   (*ht)->search = _search;
   (*ht)->first = _first;
   (*ht)->next = _next;
   (*ht)->stats = _stats;
   (*ht)->size = _size;
   (*ht)->destroy = _destroy;
}

u_int32_t _size(struct htable* ht)
{
   return ht->num_items;
}

void _stats(struct htable* ht)
{
   int hits[MAX_COUNT];
   int max = 0;
   int i, j;
   struct hlink *p;
   printf("\n\nNumItems=%d\nTotal buckets=%d\n", ht->num_items, ht->buckets);
   printf("Hits/bucket: buckets\n");
   for (i=0; i < MAX_COUNT; i++) {
      hits[i] = 0;
   }
   for (i = 0; i < (int)ht->buckets; i++) {
      p = ht->table[i];
      j = 0;
      while (p) {
         p = (struct hlink *)(p->next);
         j++;
      }
      if (j > max) {
         max = j;
      }
      if (j < MAX_COUNT) {
         hits[j]++;
      }
   }
   for (i=0; i < MAX_COUNT; i++) {
      printf("%2d:           %d\n",i, hits[i]);
   }
   printf("max hits in a bucket = %d\n", max);
}

void _grow_table(struct htable* ht)
{
   /* Setup a bigger table */
   struct htable *big = (struct htable *)malloc(sizeof(struct htable));
   //init(&big, ht->loffset, ht->hashlength, 31);
   
   big->loffset = ht->loffset;
   big->hashlength  = ht->hashlength;
   big->mask = ht->mask << 1 | 1;
   big->rshift = ht->rshift - 1;
   big->num_items = 0;
   big->buckets = ht->buckets * 2;
   big->max_items = big->buckets * 4;
   big->table = (struct hlink **)malloc(big->buckets * sizeof(struct hlink *));
   memset(big->table, 0, big->buckets * sizeof(struct hlink *));
   big->walkptr = NULL;
   big->walk_index = 0;

   big->hash_index = _hash_index;
   big->grow_table = _grow_table;
   big->insert = _insert;
   big->lookup = _lookup;
   big->search = _search;
   big->first = _first;
   big->next = _next;
   big->stats = _stats;
   big->size = _size;
   big->destroy = _destroy;
   
   /* Insert all the items in the new hash table */
   //Dmsg1(100, "Before copy num_items=%d\n", num_items);
   /*
    * We walk through the old smaller tree getting items,
    * but since we are overwriting the colision links, we must
    * explicitly save the item->next pointer and walk each
    * colision chain ourselves.  We do use next() for getting
    * to the next bucket.
    */

   void *item;
   for (item = ht->first(ht); item; ) {
      void *ni = ((struct hlink *)((char *)item + ht->loffset))->next;  /* save link overwritten by insert */
      big->insert(big, ((struct hlink *)((unsigned char *)item + ht->loffset))->key, item);
      if (ni) {
         item = (void *)((char *)ni - ht->loffset);
      } else {
         ht->walkptr = NULL;
         item = ht->next(ht);
      }
   }
   //Dmsg1(100, "After copy new num_items=%d\n", big->num_items);
   if (ht->num_items != big->num_items) {
      printf("Big problems num_items mismatch!!!!!!");
	  assert(0);
   }
   free(ht->table);
   memcpy(ht, big, sizeof(struct htable));  /* move everything across */
   free(big);
   //Dmsg0(100, "Exit grow.\n");
}

int _insert(struct htable* ht, unsigned char *key, void *item)
{
   struct hlink *hp;

   ht->index = (*(uint64_t*) key* 2654435761U) & ht->mask;
   
   hp = (struct hlink *)(((char *)item) + ht->loffset);
   hp->next = ht->table[ht->index]; /*some pro*/
   hp->key = key;
   ht->table[ht->index] = hp;

   if (++(ht->num_items) >= ht->max_items) {
      //printf("%s,%d:grow_table_hash num_items=%d max_items=%d\n", __FILE__,__LINE__,num_items, max_items);
      ht->grow_table(ht);
   }
   return 1;
}

void* _lookup(struct htable* ht, unsigned char *key)
{
   //hash_index(key);
   ht->index = (*(uint64_t*) key* 2654435761U) & ht->mask;

   struct hlink* hp;
   for (hp = ht->table[ht->index]; hp; hp = (struct hlink*)hp->next)
   {
     if(*(uint64_t *)(key)  ==  *(uint64_t *)(hp->key)) {
         return ((char *)hp) - ht->loffset;
      }
   }
   return NULL;
}

void* _search(struct htable* ht, unsigned char *key)
{
   //hash_index(key);
   ht->index = (*(uint64_t*) key* 2654435761U) & ht->mask;
   struct hlink *hp;
   for (hp = ht->table[ht->index]; hp; hp = hp->next) 
   {
      if(*(uint64_t *)(key)  ==  *(uint64_t *)( hp->key))
		 return hp;
  }
  return NULL;

}

void* _next(struct htable* ht)
{
   //Dmsg1(100, "Enter next: walkptr=0x%x\n", (unsigned)walkptr);
   if (ht->walkptr) {
      ht->walkptr = (struct hlink *)((ht->walkptr)->next);
   }
   while (!ht->walkptr && ht->walk_index < ht->buckets) {
      ht->walkptr = ht->table[ht->walk_index++];
      if (ht->walkptr) {
         //Dmsg3(100, "new walkptr=0x%x next=0x%x inx=%d\n", (unsigned)walkptr,(unsigned)(walkptr->next), walk_index-1);
		 printf("new walkptr=0x%x next=0x%x inx=%d\n", (unsigned)ht->walkptr,(unsigned)((ht->walkptr)->next), ht->walk_index-1);
	  }
   }
   if (ht->walkptr) {
      //Dmsg2(100, "next: rtn 0x%x walk_index=%d\n",(unsigned)(((char *)walkptr)-loffset), walk_index);
      return ((char *)ht->walkptr) - ht->loffset;
   }
   //Dmsg0(100, "next: return NULL\n");
   return NULL;
}

void* _first(struct htable* ht)
{
   //Dmsg0(100, "Enter first\n");
   ht->walkptr = ht->table[0];                /* get first bucket */
   ht->walk_index = 1;                    /* Point to next index */
   while (!ht->walkptr && ht->walk_index < ht->buckets) {
      ht->walkptr = ht->table[ht->walk_index++];  /* go to next bucket */
      if (ht->walkptr) {
         //Dmsg3(100, "first new walkptr=0x%x next=0x%x inx=%d\n", (unsigned)walkptr,(unsigned)(walkptr->next), walk_index-1);
      }
   }
   if (ht->walkptr) {
      return ((char *)ht->walkptr) - ht->loffset;
   }
   //Dmsg0(100, "Leave first walkptr=NULL\n");
   return NULL;
}

/* Destroy the table and its contents */
void _destroy(struct htable* ht)
{
   	void *ni;
   	void *li = ht->first(ht);
   	do {
      	ni = ht->next(ht);
      	free(li);
      	li = ni;
   	} while (ni);

   	free(ht->table);
   	ht->table = NULL;
   	if (ht)
   		free(ht);
   //Dmsg0(100, "Done destroy.\n");
}
