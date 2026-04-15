#ifndef LOCAL_COMPRESSION_PHASE_H_
#define LOCAL_COMPRESSION_PHASE_H_

/*
 * zstd 解压缩函数
 * 用于恢复时解压本地压缩的数据块
 */
int zstd_decompress(const unsigned char* input, int input_len,
                    unsigned char* output, int output_len);

#endif /* LOCAL_COMPRESSION_PHASE_H_ */
