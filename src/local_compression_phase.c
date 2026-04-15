#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "common.h"
#include <zstd.h>

static pthread_t local_compression_t;

/* 本地压缩阈值：小于此大小的块不压缩 */
#define LOCAL_COMPRESSION_MIN_SIZE 0

/* zstd 压缩级别 */
#define ZSTD_COMPRESSION_LEVEL 3

/*
 * 对普通数据块进行本地压缩
 * 差量块不进行压缩，因为已经压缩过一次
 */
static int compress_chunk(struct chunk* c) {
    /* 只压缩普通数据块（非差量块） */
    if (c->delta != NULL) {
        /* 这是差量块，已经经过差量压缩，不再进行本地压缩 */
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

    /* 跳过不会被存储的块（真正的重复块）
     * c->id == TEMPORARY_ID 表示这是新块，会被存储
     * c->id != TEMPORARY_ID 表示这是重复块，不会被存储
     */
    if (c->id != TEMPORARY_ID) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 太小的块不值得压缩 */
    if (c->size < LOCAL_COMPRESSION_MIN_SIZE) {
        jcr.local_skipped_chunk_num++;
        return 0;
    }

    /* 统计压缩前大小 */
    jcr.chunk_size_before_local_compression += c->size;
    jcr.total_size_for_local_compression += c->size;

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

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
        /* 记录原始大小 */
        int original_size = c->size;

        /* 释放原始数据 */
        free(c->data);

        /* 设置压缩后的数据 */
        c->data = compressed_buffer;
        c->size = compressed_size;
        /* 注意：不要设置 local_compressed_contents，避免重复释放 */
        c->size_after_local_compression = original_size;  /* 存储原始大小 */

        /* 统计压缩节省的大小 */
        jcr.local_compressed_size += (original_size - compressed_size);

        /* 统计被压缩的 chunk 数量 */
        jcr.local_compressed_chunk_num++;
        
        printf("DEBUG: compressed chunk, fp=%02x%02x..., original=%d, compressed=%d\n", 
               c->fp.hash[0], c->fp.hash[1], original_size, compressed_size);
    } else {
        /* 压缩后反而更大，不使用压缩数据 */
        free(compressed_buffer);

        /* 统计未被压缩的 chunk 数量 */
        jcr.local_uncompressed_chunk_num++;
    }

    TIMER_END(1, jcr.local_compression_time);

    return 0;
}

/*
 * zstd 解压缩函数
 * 用于恢复时解压本地压缩的数据块
 */
int zstd_decompress(const unsigned char* input, int input_len,
                    unsigned char* output, int output_len) {
    size_t const dSize = ZSTD_decompress(output, output_len, input, input_len);

    if (ZSTD_isError(dSize)) {
        fprintf(stderr, "ZSTD decompression failed: %s\n", ZSTD_getErrorName(dSize));
        return -1;
    }

    return (int)dSize;
}

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

        /* 对普通数据块进行本地压缩 */
        /* 差量块（c->delta != NULL）不会被压缩 */
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
