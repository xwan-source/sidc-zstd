
#ifndef DDELTA_H_
#define DDELTA_H_

#include "htable.h"

#define SUBCHUNK_DEFAULT_SIZE 2*1024

struct DeltaRecord    {
	//char 		szData[SUBCHUNK_DEFAULT_SIZE];	
	u_int64_t 	nHash; // the chunk fingerprint
	//u_int64_t 	nGear; //the hash of  the last CDC window
	
	u_int32_t 	nOffset;//the offset of the string in the chunk
	/* the first character of the string is **buf[nOffset], and 
	* there're nOffset characters before the string in the **buf.
	*/
	u_int32_t 	nLength;//string length
	u_int16_t 	DupFlag; // 1 dup, 0 sim, 2 beg or end
	u_int32_t 	nSimID; /* used in the input chunk/file to indicate which string in the 
						* base chunk/file it is indentical to
						*/
	struct hlink   	  psNextSubCnk;
};// to a string

/* to represent an identical string to the base, 8 bytes, flag=0 */
struct DeltaUnit1 {
	u_int32_t	flag_length; //flag & length
	/* the first bit for the flag, the other 31 bits for the length */

	u_int32_t 	nOffset;
};

/* to represent an  string not identical to the base, 4 bytes, flag=1 */
struct DeltaUnit2 {
	u_int32_t 	flag_length; //flag & length
	/* the first bit for the flag, the other 31 bits for the length */
};  

struct DeltaIO {
	u_int32_t   nDeFlag; //+two bits+: 0, dup, 1 new, 2 reserved for self duplicate...
	u_int32_t 	nLength; //+14 bits +: the maximal number should be 16384
	u_int32_t 	nOffset; //+14bits +: the maximal number should be 65536
};

#define DATASET 0 /* 0 tarfile, 1 post dedup */

#if DATASET	//post dedup
#define STRAVG  31
#else 		//tarfile
#define STRAVG  63
#endif

#define STRMIN  ((STRAVG+1)/2)
#define STRMAX  ((STRAVG+1)*4)

#define HASH 0 //0 spookyhash; 1 xxhash

#define POST_DEDUP 1 //0 or 1

void boxcar_init();
int rolling_gear(unsigned char *p, int n, int *cut);
int chunk_gear(unsigned char *p, int n);
int rolling_gear_v3(unsigned char *p, int n, int num_of_chunks, int *cut);
int dDelta_Encode( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize);
int eDelta_Encode_v3( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize );	
int dDelta_Decode( uint8_t* deltaBuf,u_int32_t deltaSize, 
						uint8_t* baseBuf, u_int32_t baseSize,
		 				uint8_t* outBuf,   u_int32_t *outSize);
int Chunking(unsigned char *data, int len, struct DeltaRecord *subChunkLink);
int Chunking_v3(unsigned char *data, int len, int num_of_chunks, struct DeltaRecord *subChunkLink);
uint64_t weakHash(unsigned char *buf, int len);
#endif
