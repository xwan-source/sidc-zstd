#include <stdint.h>/*uint32_t*/
#include <stdlib.h> /*size_t*/
#include <string.h>/*memset*/
#include "../destor.h"
//#include "../index/spooky_c.h"
#include "rabin_chunking.h"
#include "murmur3.h"
#include "../jcr.h"
#include "spooky_c.h"
#include "xxhash.h"
#include <openssl/sha.h>
#include <openssl/md5.h>
#include "md5.h"


#define GSIZE32BIT  0xffffffffLL
#define COMP_SIZE 8

#define SeedLength 64
#define SymbolTypes 256
#define MD5Length 16

#define SLIDE(m,fp,bufPos,buf) do{	\
	    unsigned char om;   \
	    u_int64_t x;	 \
		if (++bufPos >= size)  \
            bufPos = 0;				\
        om = buf[bufPos];		\
        buf[bufPos] = m;		 \
		fp ^= U[om];	 \
		x = fp >> shift;  \
		fp <<= 8;		   \
		fp |= m;		  \
		fp ^= T[x];	 \
}while(0)

#define STRAVG  63
#define STRMIN  ((STRAVG+1)/2)
#define STRMAX  ((STRAVG+1)*4)

typedef unsigned int UINT32;
typedef unsigned long long int UINT64;

uint64_t *matrix;

UINT32 GEAR[256];

enum {
	size = 48
};
UINT64 fp;
int bufpos;
UINT64 U[256];
unsigned char buf[size];
int shift;
UINT64 T[256];
UINT64 poly;

char *eFiles[] = { ".pdf", ".rmv", "ra", ".bmp", ".vmem", ".vmdk", ".jpeg",
		".rmvb", ".exe", ".mtv", "\\Program Files", "C:\\" };

long int MiAi[256][2] =
{
5652377,  45673472, 24423617, 146512310, 16341615, 142364781,
3526234, 1225434201,  654410,324130,  3124312, 359840125,
35, 15254201,  61,41870,  212, 3340125,
121, 44122, 551, 8431641, 610, 824311,
367, 254201,  651,7632410,  319, 7540125,
129, 115422, 551, 34431741, 633, 424311,
11, 31, 17,5, 23, 37,
71,97,37,241,247,257,
137,2391,431,831,2137,4313,
173, 3022467, 523, 4507221, 753, 6750467,
143, 412234, 372, 3174123, 637, 324123,
821, 25420451, 107,31023,  423, 3512545,
667, 1541242, 851, 843541741, 453, 3244311,
964, 223401,  910,3233410,  223, 354012523,
827,12322, 751, 843178741, 456, 438911,
};

size_t _last_pos;
size_t _cur_pos;

unsigned int _num_chunks;

int NUMFEATURE = 4;

int ae_avg_size, ae_window_size;

int expected_chunk_size = 8192;
uint64_t chunkMask = 0x0000d90f03530000;
uint64_t chunkMask2 = 0x0000d90003530000;

int MaxChunkSize = 65536;
int MinChunkSize = 2048;

const char bytemsb[0x100] = { 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5,
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, };

/***********************************************the rabin**********************************************/
static uint32_t fls32(UINT32 v) {
	if (v & 0xffff0000) {
		if (v & 0xff000000)
			return 24 + bytemsb[v >> 24];
		else
			return 16 + bytemsb[v >> 16];
	}
	if (v & 0x0000ff00)
		return 8 + bytemsb[v >> 8];
	else
		return bytemsb[v];
}

static uint32_t fls64(UINT64 v) {
	UINT32 h;
	if ((h = v >> 32))
		return 32 + fls32(h);
	else
		return fls32((UINT32) v);
}

UINT64 polymod(UINT64 nh, UINT64 nl, UINT64 d) {

	int k = fls64(d) - 1;
	int i;

	d <<= 63 - k;

	if (nh) {
		if (nh & MSB64)
			nh ^= d;

		for (i = 62; i >= 0; i--)
			if (nh & ((UINT64) 1) << i) {
				nh ^= d >> (63 - i);
				nl ^= d << (i + 1);
			}
	}
	for (i = 63; i >= k; i--) {
		if (nl & (long long int) (1) << i)
			nl ^= d >> 63 - i;
	}

	return nl;
}

void polymult(UINT64 *php, UINT64 *plp, UINT64 x, UINT64 y) {

	int i;

	UINT64 ph = 0, pl = 0;
	if (x & 1)
		pl = y;
	for (i = 1; i < 64; i++)
		if (x & ((long long int) (1) << i)) {
			ph ^= y >> (64 - i);
			pl ^= y << i;
		}
	if (php)
		*php = ph;
	if (plp)
		*plp = pl;
}

UINT64 append8(UINT64 p, unsigned char m) {
	return ((p << 8) | m) ^ T[p >> shift];
}

UINT64 slide8(unsigned char m) {
	unsigned char om;
	if (++bufpos >= size)
		bufpos = 0;
	om = buf[bufpos];
	buf[bufpos] = m;
	return fp = append8(fp ^ U[om], m);
}

UINT64 polymmult(UINT64 x, UINT64 y, UINT64 d) {

	UINT64 h, l;
	polymult(&h, &l, x, y);
	return polymod(h, l, d);
}

void calcT(UINT64 poly) {

	int j;
	UINT64 T1;

	int xshift = fls64(poly) - 1;
	shift = xshift - 8;
	T1 = polymod(0, (long long int) (1) << xshift, poly);
	for (j = 0; j < 256; j++) {
		T[j] = polymmult(j, T1, poly) | ((UINT64) j << xshift);
	}
}

void rabinpoly_init(UINT64 p) {
	poly = p;
	calcT(poly);
}

uint64_t *gearMatrix;

int kArray[12] = {
        1007,
        459326,
        48011,
        935033,
        831379,
        530326,
        384145,
        687288,
        846569,
        654457,
        910678,
        48431
};
int bArray[12] = {
        1623698648,
        -1676223803,
        -687657152,
        1116486740,
        115856562,
        -2129903346,
        897592878,
        -148337918,
        -1948941976,
        1506843910,
        -1582821563,
        1441557442,
};
uint64_t *maxList;

void window_init(UINT64 poly) {

	int i;
	UINT64 sizeshift;

	rabinpoly_init(poly);
	fp = 0;
	bufpos = -1;
	sizeshift = 1;
	for (i = 1; i < size; i++)
		sizeshift = append8(sizeshift, 0);
	for (i = 0; i < 256; i++)
		U[i] = polymmult(i, sizeshift, poly);
	memset((char*) buf, 0, sizeof(buf));
}

void windows_reset() {
	fp = 0;
}

void chunkAlg_init() {
	window_init(FINGERPRINT_PT);
	_last_pos = 0;
	_cur_pos = 0;
	windows_reset();
	_num_chunks = 0;
}

void gear_init(){
    char seed[32];
	int i,j;
	for(i = 0 ; i < 256; i++){
		for(j=0; j<32;j++){
			seed[j] = i;
		}
		GEAR[i] = 0;
		{
    		unsigned char digest[16];
    		MD5_CTX ctx;
 			MD5_Init(&ctx);
			MD5_Update(&ctx, seed, 32);
			MD5_Final(digest,&ctx);
			memcpy( &GEAR[i], digest, 7);
		}
	}
}

void ae_init() {
    ae_avg_size = destor.chunk_avg_size / 1.7183;
    ae_window_size = destor.chunk_avg_size / (1.7183*6);
}

void odessInit() {
    gearMatrix = (uint64_t *) malloc(sizeof(uint64_t) * SymbolTypes);
    maxList = (uint64_t *) malloc(sizeof(uint64_t) * 12);
    memset(maxList, 0, sizeof(uint64_t) * 12);
    char seed[SeedLength];
    for (int i = 0; i < SymbolTypes; i++) {
        for (int j = 0; j < SeedLength; j++) {
            seed[j] = i;
        }

        gearMatrix[i] = 0;
        char md5_result[MD5Length];
        md5_state_t md5_state;
        md5_init(&md5_state);
        md5_append(&md5_state, (md5_byte_t *) seed, SeedLength);
        md5_finish(&md5_state, (md5_byte_t *) md5_result);

        memcpy(&gearMatrix[i], md5_result, sizeof(uint64_t));
    }
}

int ae_chunk_data(unsigned char *p, int n) {

    unsigned char *p_curr, *max_str = p;
    int comp_res = 0, i, end, max_pos = 0;
    if(n < ae_avg_size - COMP_SIZE)
        return n;
    end = n - COMP_SIZE;
    i = 1;
    p_curr = p + 1;
    while (i < end){
		int end_pos = max_pos + ae_avg_size;
		if (end_pos > end)
				end_pos = end;
       	while(*((uint64_t *)p_curr) <= *((uint64_t *)max_str) && i < end_pos){
           p_curr++;
		   i++;
        }
        uint64_t _cur = *((uint64_t *) p_curr);
    	uint64_t _max = *((uint64_t *) max_str);
		if (_cur > _max) {
			max_str = p_curr;
			max_pos = i;
		}
		else
			return end_pos;
		p_curr++;
		i++;
    }
    return n;
}

int rabin_chunk_data(unsigned char *p, int n) {

	UINT64 f_break = 0;
	UINT64 count = 0;
	UINT64 fp = 0;
	int i = 1, bufPos = -1;

	unsigned char om;
	u_int64_t x;

	unsigned char buf[128];
	memset((char*) buf, 0, 128);

	if (n <= destor.chunk_min_size)
		return n;
	else
		i = destor.chunk_min_size;

	while (i <= n) {

		SLIDE(p[i - 1], fp, bufPos, buf);
		if (((fp & (destor.chunk_avg_size - 1)) == BREAKMARK_VALUE)
			|| i >= destor.chunk_max_size
				|| i == n) {
			break;
		} else
			i++;
	}
	return i;
}

int fastcdc_chunk_data(unsigned char *p, int n) {

        uint64_t fingerprint = 0, digest;
        int i = MinChunkSize, Mid = MinChunkSize + expected_chunk_size;
        //return n;

        if (n <= MinChunkSize) //the minimal  subChunk Size.
            return n;
        //windows_reset();
        if (n > MaxChunkSize)
            n = MaxChunkSize;
        else if (n < Mid)
            Mid = n;
        while (i < Mid) {
            fingerprint = (fingerprint << 1) + (gearMatrix[p[i]]);
            if ((!(fingerprint & chunkMask))) { //AVERAGE*2, *4, *8
                return i;
            }
            i++;
        }
        while (i < n) {
            fingerprint = (fingerprint << 1) + (gearMatrix[p[i]]);
            if ((!(fingerprint & chunkMask2))) { //Average/2, /4, /8
                return i;
            }
            i++;
        }
        return i;
}


void super_feature_rabin(unsigned char* p, int n, struct sketch* sketches)
{
    int number = 3;
	UINT64 SFeature, Feature[124];
	UINT64 fingerprint = 0;
	int i=48, j, bufPos=-1, jj, ii;
    int32_t feature_size;

	unsigned char om, Fbuf[256], *Tempbuf;
	unsigned char buf[128];
	memset ((char*) buf,0, 128);
	memset((char*)Feature, 0, sizeof(Feature));

	for(j = 48; j >= 2; j--){
		SLIDE(p[i-j], fingerprint, bufPos, buf);
	}

	while(i <= n)
	{
		SLIDE(p[i-1], fingerprint, bufPos, buf);
		for(jj = 0; jj < number*NUMFEATURE; jj++)
		{
			SFeature = (MiAi[jj][0]*fingerprint + MiAi[jj][1]) & GSIZE32BIT; //&GSIZE32BIT;
			if(SFeature > Feature[jj]){
				Feature[jj] = SFeature;
 			}
		}
		i++;
 	}

    jj = 0;
    feature_size = 8*NUMFEATURE;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
    uint64_t temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf1 = temp_hash;

    jj++;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
	temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf2 = temp_hash;

    jj++;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
	temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf3 = temp_hash;

  	return ;
}

void super_feature_gear(unsigned char* p, int n, struct sketch* sketches)
{
    int number = 3;
	UINT64 SFeature, Feature[124];
	UINT64 fingerprint = 0;
	int i=48, j, bufPos=-1, jj, ii;
    int32_t feature_size;

	unsigned char om, Fbuf[256], *Tempbuf;
	unsigned char buf[128];
	memset ((char*) buf,0, 128);
	memset((char*)Feature, 0, sizeof(Feature));

	for(j = 48; j >= 2; j--){
		SLIDE(p[i-j], fingerprint, bufPos, buf);
	}

	while(i <= n)
	{
		SLIDE(p[i-1], fingerprint, bufPos, buf);
		for(jj = 0; jj < number*NUMFEATURE; jj++)
		{
			SFeature = (MiAi[jj][0]*fingerprint + MiAi[jj][1]) & GSIZE32BIT; //&GSIZE32BIT;
			if(SFeature > Feature[jj]){
				Feature[jj] = SFeature;
 			}
		}
		i++;
 	}

    jj = 0;
    feature_size = 8*NUMFEATURE;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
    uint64_t temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf1 = temp_hash;

    jj++;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
	temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf2 = temp_hash;

    jj++;
    Tempbuf = (unsigned char*) (Feature + NUMFEATURE*jj);
	temp_hash = spooky_hash64(Tempbuf, feature_size, 12345678);
	sketches->sf3 = temp_hash;

  	return ;
}

void finesse_rabin(unsigned char* p, int n, struct sketch* sketches)
{
    int subregion_num, subregion_size = n/destor.super_feature_subregion_num;
	UINT64 Feature[24];
	UINT64 fingerprint;
	int i, j, bufPos, jj, ii, endpos;

	unsigned char om, Fbuf[256], *Tempbuf;
	unsigned char buf[128];
    memset ((char*) buf, 0, 128);
    memset((char*)Feature, 0, sizeof(Feature));

    i = 48;
    bufPos=-1;
    fingerprint = 0;
    endpos = 0;

    for(j = 48; j >= 2; j--)
        SLIDE(p[i-j], fingerprint, bufPos, buf);

    for (subregion_num = 0, jj = 0; subregion_num < destor.super_feature_subregion_num;
                                    subregion_num++, jj++) {
        endpos += subregion_size;
		if (endpos > n)
			endpos = n;

        while (i < endpos) {
            SLIDE(p[i-1], fingerprint, bufPos, buf);
            if (fingerprint > Feature[jj])
                Feature[jj] = fingerprint;
			i++;
        }
    }

    /* grouping phase */
    get_sfs(sketches, Feature);
}

void finesse_gear(unsigned char* p, int n, struct sketch* sketches)
 {
	int subregion_num, subregion_size = n/destor.super_feature_subregion_num;
	UINT64 Feature[24];
	UINT64 fingerprint;
	int i, j, bufPos, jj, ii, endpos;

	unsigned char om, Fbuf[256], *Tempbuf;
	unsigned char buf[128];
    memset ((char*) buf, 0, 128);
    memset((char*)Feature, 0, sizeof(Feature));

	i = 1;
    bufPos=-1;
    fingerprint = 0;
    endpos = 0;

	 for (subregion_num = 0, jj = 0; subregion_num < destor.super_feature_subregion_num;
									 subregion_num++, jj++) {
		 endpos += subregion_size;
		 if (endpos > n)
			 endpos = n;

		 while (i < endpos) {
			 fingerprint = (fingerprint<<1) + GEAR[p[i]];
			 if (fingerprint > Feature[jj])
				 Feature[jj] = fingerprint;
			 i++;
		 }
	 }

	/* grouping phase */
	get_sfs(sketches, Feature);
 }

void odessCalculation(unsigned char* buffer, uint64_t length, struct sketch* SFs){
    uint64_t hashValue = 0;
    for (uint64_t i = 0; i < length; i++) {
        hashValue = (hashValue << 1) + gearMatrix[buffer[i]];
        if (!(hashValue & 0x0000400303410000)) { // sampling mask
            for (int j = 0; j < 12; j++) {
                uint64_t transResult = (hashValue * kArray[j] + bArray[j]);
                if (transResult > maxList[j])
                    maxList[j] = transResult;
            }
        }
    }

    SFs->sf1 = XXH64(&maxList[0 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    SFs->sf2 = XXH64(&maxList[1 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    SFs->sf3 = XXH64(&maxList[2 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
    memset(maxList, 0, sizeof(uint64_t)*12);
}

inline void get_sfs(struct sketch* sketches, UINT64* features) {
	uint64_t temp_hash;
	UINT64 TempFeatures[4];
	int feature_size = 4*sizeof(UINT64);

	TempFeatures[0] = *(features);
	TempFeatures[1] = *(features+3);
	TempFeatures[2] = *(features+6);
	TempFeatures[3] = *(features+9);
	temp_hash = spooky_hash64((unsigned char*)TempFeatures, feature_size, 12345678);
	sketches->sf1 = temp_hash;

	TempFeatures[0] = *(features+1);
	TempFeatures[1] = *(features+4);
	TempFeatures[2] = *(features+7);
	TempFeatures[3] = *(features+10);
	temp_hash = spooky_hash64((unsigned char*)TempFeatures, feature_size, 12345678);
	sketches->sf2 = temp_hash;

	TempFeatures[0] = *(features+2);
	TempFeatures[1] = *(features+5);
	TempFeatures[2] = *(features+8);
	TempFeatures[3] = *(features+11);
	temp_hash = spooky_hash64((unsigned char*)TempFeatures, feature_size, 12345678);
	sketches->sf3 = temp_hash;
}

uint64_t *getMatrix() {
        return gearMatrix;
}
