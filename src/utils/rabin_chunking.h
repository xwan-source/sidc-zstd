/* chunking.h
 the main fuction is to chunking the file!
 */

#ifndef RABIN_H_
#define RABIN_H_

#include "../destor.h"

#define MSB64 0x8000000000000000LL
#define MAXBUF (128*1024)

#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78

void ae_init();
void gear_init();
void chunkAlg_init();
void windows_reset();

void odessInit();


int rabin_chunk_data(unsigned char *p, int n);
int ae_chunk_data(unsigned char *p, int n);
int fastcdc_chunk_data(unsigned char *p, int n);

void super_feature_rabin(unsigned char* p, int n, struct sketch* sketches);
void finesse_rabin(unsigned char* p, int n, struct sketch* sketches);
void finesse_gear(unsigned char* p, int n, struct sketch* sketches);

void odessCalculation(uint8_t* buffer, uint64_t length, struct sketch* SFs);
uint64_t *getMatrix();

#endif
