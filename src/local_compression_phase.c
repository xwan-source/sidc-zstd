#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "common.h"
#include <zstd.h>

static pthread_t local_compression_t;

/* zstd 压缩级别 */
#define ZSTD_COMPRESSION_LEVEL 3

/*
 * 对普通数据块进行本地压缩
 * 差量块和重复块不进行压缩
 */
static int compress_chunk(struct chunk* c) {
    /* 跳过差量块 */
    if (c->delta != NULL) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 跳过重复块（不会被存储的块）
     * c->id == TEMPORARY_ID 表示这是新块，会被存储
     */
    if (c->id != TEMPORARY_ID) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 统计压缩前大小 */
    jcr.chunk_size_before_local_compression += c->size;

    /* 计算压缩后最大可能大小 */
    size_t const compressed_buff_size = ZSTD_compressBound(c->size);
    void* compressed_buffer = malloc(compressed_buff_size);
    if (compressed_buffer == NULL) {
        fprintf(stderr, "Failed to allocate memory for local compression\n");
        return -1;
    }

    /* 使用 zstd 压缩 */
    size_t const compressed_size = ZSTD_compress(compressed_buffer, compressed_buff_size,
                                                  c->data, c->size, ZSTD_COMPRESSION_LEVEL);

    if (ZSTD_isError(compressed_size)) {
        fprintf(stderr, "ZSTD compression failed: %s\n", ZSTD_getErrorName(compressed_size));
        free(compressed_buffer);
        return -1;
    }

    /* 只有当压缩后大小小于原始大小时才使用压缩数据 */
    if (compressed_size < c->size) {
        /* 保存原始数据大小 */
        int original_size = c->size;

        /* 释放原始数据 */
        free(c->data);

        /* 设置压缩后的数据 */
        c->data = compressed_buffer;
        c->size = compressed_size;

        /* 保存原始大小（用于存储时识别已压缩） */
        c->original_size_before_compression = original_size;

        /* 统计 */
        jcr.local_compressed_size += (original_size - compressed_size);
        jcr.local_compressed_chunk_num++;
    } else {
        /* 压缩后反而更大，不使用压缩数据 */
        free(compressed_buffer);
        c->original_size_before_compression = 0;  /* 0 表示未压缩 */
        jcr.local_uncompressed_chunk_num++;
    }

    return 0;
}

static void* local_compression_thread(void *arg) {
    struct chunk* c;
    while (1) {
        c = sync_queue_pop(delta_queue);
        if (c == NULL)
            break;

        /* 跳过信号块 */
        if (CHECK_CHUNK(c, CHUNK_FILE_START) ||
            CHECK_CHUNK(c, CHUNK_FILE_END) ||
            CHECK_CHUNK(c, CHUNK_SEGMENT_START) ||
            CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
            sync_queue_push(local_compression_queue, c);
            continue;
        }

        /* 对普通数据块进行本地压缩 */
        compress_chunk(c);

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
