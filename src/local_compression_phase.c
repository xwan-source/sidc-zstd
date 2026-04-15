
#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "common.h"
//#include <zstd.h>

static pthread_t local_compression_t;

static void* local_compression_thread(void *arg) {

	while (1) {
        struct chunk* c = sync_queue_pop(delta_queue);

        if (c == NULL)
            /* backup job finish */
            break;

        if(CHECK_CHUNK(c, CHUNK_FILE_START) 
        		|| CHECK_CHUNK(c, CHUNK_FILE_END) 
        		|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
        		|| CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
        		
        	sync_queue_push(local_compression_queue, c);
        	continue;
        }

		/*
		if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			
			if(!c->delta) {

				jcr.chunk_size_before_local_compression += c->size;
        		TIMER_DECLARE(1);
        		TIMER_BEGIN(1);

				size_t const cBuffSize = ZSTD_compressBound(c->size);
				void* compressed_buffer = malloc(cBuffSize);
				size_t const cSize = ZSTD_compress(compressed_buffer,
						cBuffSize, c->data, c->size, ZSTD_CLEVEL_DEFAULT);
				assert(!ZSTD_isError(cSize));
				
				c->local_compressed_contents = malloc(cSize);
				memcpy(c->local_compressed_contents, compressed_buffer, cSize);
				c->size_after_local_compression = cSize;
		
        		TIMER_END(1, jcr.local_compression_time);
			}
		}
		*/

        sync_queue_push(local_compression_queue, c);
    }
    sync_queue_term(local_compression_queue);
	
	return NULL;
}

void start_local_compression_phase() {

	local_compression_queue = sync_queue_new(2000);
	pthread_create(&local_compression_t, NULL, local_compression_thread, NULL);
}

void stop_local_compression_phase() {

	pthread_join(local_compression_t, NULL);
}
