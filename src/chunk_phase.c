#include "destor.h"
#include "jcr.h"
#include "utils/rabin_chunking.h"
#include "backup.h"
//#include "murmur3.h"

static pthread_t chunk_t;
static int64_t chunk_num;

static void* chunk_thread(void *arg) {
	int leftlen = 0;
	int leftoff = 0;
	unsigned char *leftbuf = malloc(DEFAULT_BLOCK_SIZE + destor.chunk_max_size);

	unsigned char *zeros = malloc(destor.chunk_max_size);
	bzero(zeros, destor.chunk_max_size);
	unsigned char *data = malloc(destor.chunk_max_size);

	struct chunk* c = NULL;

	while (1) {

		/* Try to receive a CHUNK_FILE_START. */
		c = sync_queue_pop(read_queue);

		if (c == NULL) {
			sync_queue_term(chunk_queue);
			break;
		}

		assert(CHECK_CHUNK(c, CHUNK_FILE_START));
		sync_queue_push(chunk_queue, c);

		/* Try to receive normal chunks. */
		c = sync_queue_pop(read_queue);
		if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
			memcpy(leftbuf, c->data, c->size);
			leftlen += c->size;
			free_chunk(c);
			c = NULL;
		}
		
		while (1) {
			/* c == NULL indicates more data for this file can be read. */
			if ((leftlen < destor.chunk_max_size) && c == NULL) {
				c = sync_queue_pop(read_queue);
				if (!CHECK_CHUNK(c, CHUNK_FILE_END)) {
					memcpy(leftbuf, leftbuf + leftoff, leftlen);
					leftoff = 0;
					memcpy(leftbuf + leftlen, c->data, c->size);
					leftlen += c->size;
					free_chunk(c);
					c = NULL;
				}
			}

			if (leftlen == 0) {
				assert(c);
				break;
			}
            
            int chunk_size = 0, chunk_len = 0, i, isEnd;
    
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);
			if (destor.chunk_algorithm == CHUNK_RABIN)
				chunk_size = rabin_chunk_data(leftbuf + leftoff, leftlen);
			else
				chunk_size = fastcdc_chunk_data(leftbuf + leftoff, leftlen);
			TIMER_END(1, jcr.chunk_time);

            struct chunk *nc = NULL;
			nc = new_chunk(chunk_size);
            
			memcpy(nc->data, leftbuf + leftoff, chunk_size);
			leftlen -= chunk_size;
			leftoff += chunk_size;

            chunk_num++;
			sync_queue_push(chunk_queue, nc);
		}

		sync_queue_push(chunk_queue, c);
		leftoff = 0;
		windows_reset();
		c = NULL;
	}

	free(leftbuf);
	free(zeros);
	free(data);
	return NULL;
}

void start_chunk_phase() {
	assert(destor.chunk_avg_size > destor.chunk_min_size);
	assert(destor.chunk_avg_size < destor.chunk_max_size);

	chunkAlg_init();
    ae_init();
	gear_init();
	odessInit();
	chunk_queue = sync_queue_new(2000);
	pthread_create(&chunk_t, NULL, chunk_thread, NULL);
}

void stop_chunk_phase() {
	pthread_join(chunk_t, NULL);
}
