#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "common.h"
#include <zstd.h>

static pthread_t local_compression_t;

/*
 * 本地压缩阶段
 * 注意：压缩已经移到存储时进行，这里只保留流水线结构
 * 和必要的统计信息
 */
static int process_chunk(struct chunk* c) {
    /* 跳过差量块 */
    if (c->delta != NULL) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 跳过信号块 */
    if (CHECK_CHUNK(c, CHUNK_FILE_START) ||
        CHECK_CHUNK(c, CHUNK_FILE_END) ||
        CHECK_CHUNK(c, CHUNK_SEGMENT_START) ||
        CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 跳过不会被存储的块（真正的重复块） */
    if (c->id != TEMPORARY_ID) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 统计会被存储的块 */
    jcr.chunk_size_before_local_compression += c->size;
    jcr.total_size_for_local_compression += c->size;
    jcr.local_compressed_chunk_num++;

    /* 
     * 注意：实际的压缩在存储时进行（containerstore.c）
     * 这里只进行统计，不修改 chunk 的数据
     */

    return 0;
}

static void* local_compression_thread(void *arg) {
    struct chunk* c;
    while (1) {
        c = sync_queue_pop(local_compression_queue);
        if (c == NULL)
            break;

        process_chunk(c);

        sync_queue_push(filter_queue, c);
    }

    sync_queue_term(filter_queue);
    return NULL;
}

void start_local_compression_phase() {
    local_compression_queue = sync_queue_new(2000);
    filter_queue = sync_queue_new(2000);
    pthread_create(&local_compression_t, NULL, local_compression_thread, NULL);
}

void stop_local_compression_phase() {
    pthread_join(local_compression_t, NULL);
}
