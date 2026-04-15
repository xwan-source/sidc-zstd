#include <openssl/sha.h>
#include <openssl/md5.h>
#include "../utils/spooky_c.h"
#include "../utils/xxhash.h"
#include <Ddelta.h>

uint64_t nStage1=0;
uint64_t nStage2=0;
uint64_t nStage3=0;

uint64_t nStage3_beg=0;
uint64_t nStage3_end=0;

uint64_t nStage4=0;
uint64_t nStage5=0;

uint32_t dDelta_AVG_Size;
uint32_t dDelta_MAX_Size;
uint32_t dDelta_MIN_Size;

UINT32 GEAR[256];

void boxcar_init(){
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
	dDelta_AVG_Size = 64-1;
	dDelta_MAX_Size = 256;
	dDelta_MIN_Size = 32;
}

int rolling_gear(unsigned char *p, int n, int *cut) {
	UINT32 fingerprint = 0;
	int i = 0, count = 0;
	cut[count++] = 0;
	if(n<=STRMAX){	
		cut[count] = n;
		return count;
	}
	i+=STRMIN+1;

	while(i < n)
	{		
		fingerprint = (fingerprint<<1) +GEAR[p[i]];
		if(i <= (cut[count-1] + STRMAX)){
			if( !(fingerprint & STRAVG)){
				cut[count++] = i;
				i+=STRMIN;
			}
		}
		else{
			cut[count++] = i;
			i+=STRMIN;
		}
		i++;
    }
    
    if(n < (cut[count-1] + STRAVG)){
		count = count - 1;
    }
	cut[count] = i;
    
    return count;
}

int chunk_gear(unsigned char *p, int n) {
	UINT32 fingerprint=0;
	int i=STRMIN+1;
	if(n<=STRMAX){	
		return n;
	}

	while(i <= n)
	{		
		fingerprint = (fingerprint<<1) +GEAR[p[i]];
		if(i <= STRMAX){
			if( !(fingerprint & STRAVG))
				return i;
		}
		else{
			return i;
		}
		i++;
    }
}

//jump by STRMIN bytes
/* different from rolling_gear_v2, where @n indicates the bytes left in @p that can
* be chunked. 
* And rolling_gear_v2 is abandoned.
*/
int rolling_gear_v3(unsigned char *p, int n, int num_of_chunks, int *cut)
{
	UINT32 fingerprint=0;
	int i=0,count=0;
	cut[count++] = 0;

	i+=STRMIN+1;

	int j=0;
	while( j < num_of_chunks ){
		if(i < n){
			fingerprint = (fingerprint<<1) +GEAR[p[i]];
			if(i <= (cut[count-1] + STRMAX)){
				if( !(fingerprint & STRAVG )){
					/* this requires the last few bits of fingerprint to be all 0 */
					cut[count++] = i;
					i+=STRMIN;
					fingerprint=0;
					j++;
				}
			}
			else{
				cut[count++] = i;
				i+=STRMIN;
				fingerprint=0;
				j++;
			}
			i++;
		}
		else{
			while( j < num_of_chunks ){
				cut[count++] = n;
				j++;
			}

			break;
		}	
    }
    
    return cut[count-1];
}


/* flag=0 for 'D', 1 for 'S' */
static inline void set_flag(void *record, u_int32_t flag)
{
	u_int32_t *flag_length=(u_int32_t *)record;
	if(flag==0){
		(*flag_length) &= ~(u_int32_t)0>>1;
	}
	else{
		(*flag_length) |= (u_int32_t)1<<31;
	}
}

/* return 0 if flag=0, >0(not 1) if flag=1 */
static inline u_int32_t get_flag(void *record)
{
	u_int32_t *flag_length=(u_int32_t *)record;
	return (*flag_length) & (u_int32_t)1<<31;
}

static inline void set_length(void *record, u_int32_t length)
{
	u_int32_t *flag_length=(u_int32_t *)record;
	u_int32_t musk = (*flag_length) & (u_int32_t)1<<31;
	*flag_length = length | musk;
}

static inline u_int32_t get_length(void *record)
{
	u_int32_t *flag_length=(u_int32_t *)record;
	return (*flag_length) & ~(u_int32_t)0>>1;
}

/* 
* do the chunking work for a character stream, and store the information for the strings 
*
* @data: the character stream
* @len: the length of the stream
* @subChunkLink: to store the information for the strings 
* @return: the number of the strings
*/
int Chunking(unsigned char *data, int len, struct DeltaRecord *subChunkLink)
{
	if(len <= 0) return 0;
	int i=0;
	/* cut is the chunking points in the stream */
	int * cut = (int*)malloc((len/STRMIN +10)*sizeof(int));
	
	//===========================
	int numChunks = rolling_gear(data, len, cut);

	while( i < numChunks )
	{
		int chunkLen = cut[i+1] - cut[i];		
		subChunkLink[i].nLength = chunkLen;
		subChunkLink[i].nOffset = cut[i];/**/
		subChunkLink[i].DupFlag = 0;
		subChunkLink[i].nHash   = weakHash(data+ cut[i], chunkLen);
		//	SpookyHash::Hash64(data+ cut[i], chunkLen, 0x1af1);
		i++;
	}
	free(cut);
	return numChunks;
}

/* different from Chunking_v2, where @len indicates the bytes left in @data that can
* be chunked. and if the data left is too short to be chunked into @num_of_chunks 
* chunks, the nLength of the elements left in the @subChunkLink is set to 0. 
* And Chunking_v2 is abandoned.
*/
int Chunking_v3(unsigned char *data, int len, int num_of_chunks, struct DeltaRecord *subChunkLink)
{
	int i=0,*cut;
	/* cut is the chunking points in the stream */
	cut = (int*)malloc((num_of_chunks+1)*sizeof(int));
	

	//struct timeval TvStart1,TvEND1,TvStart2,TvEND2;
	//double tChunk,tHash;
	//gettimeofday(&TvStart1,NULL);
	//===========================
#if CHUNK == 0
	int numBytes = rolling_gear_v3(data, len, num_of_chunks, cut);
#else
	printf("only support gear to chunk!");
	exit(0);
#endif
	//===========================
	//gettimeofday(&TvEND1,NULL);	
	//tChunk = (TvEND1.tv_sec-TvStart1.tv_sec)*1000+ (TvEND1.tv_usec-TvStart1.tv_usec)/1000.0;
	//printf("Chunking time : %8.3f\n",tChunk);

	//gettimeofday(&TvStart2,NULL);
	while( i < num_of_chunks )
	{
		int chunkLen = cut[i+1] - cut[i];		
		subChunkLink[i].nLength = chunkLen;
		subChunkLink[i].nOffset = cut[i];/**/
		subChunkLink[i].DupFlag = 0;
		subChunkLink[i].nHash   = weakHash(data+ cut[i], chunkLen);
		//	SpookyHash::Hash64(data+ cut[i], chunkLen, 0x1af1);
		i++;
	}
	//gettimeofday(&TvEND2,NULL);
	//tHash = (TvEND2.tv_sec-TvStart2.tv_sec)*1000+ (TvEND2.tv_usec-TvStart2.tv_usec)/1000.0;
	//printf("Hashing time : %8.3f\n",tHash);
	//assert( cut[i-1] == len);
	free(cut);
	return numBytes;
}

/* this version is which we finally use */
int dDelta_Encode( uint8_t* newBuf, u_int32_t newSize,
		  				uint8_t* baseBuf, u_int32_t baseSize,
		  				uint8_t* deltaBuf, u_int32_t *deltaSize) {
		  				
	/***** stage 1 *****/	
	int Beg = 0, BegSize = 0, End = 0, EndSize = 0;
	if(newBuf[0] == baseBuf[0]){
		int i = 1;
		while(newBuf[i] == baseBuf[i] && i < newSize && i < baseSize) {i++;}		
		if(i>8){
			Beg = 1;
			BegSize = i;
		}
	}
		
	if(newBuf[newSize-1] == baseBuf[baseSize-1] && BegSize < baseSize){
		int i = 2;
		while(newBuf[newSize-i] == baseBuf[baseSize-i]  && i < newSize && i < baseSize) {i++;} 	
		if(i > 8){
			End = 1;
			EndSize = i - 1;
		}
	}
	nStage1+= BegSize + EndSize;
	if(newSize < BegSize + EndSize){
		EndSize = newSize - BegSize;
	}
	
	/***** stage 2 *****/
	struct DeltaRecord *BaseLink,*NewLink;
	BaseLink =(struct DeltaRecord *)malloc(sizeof(struct DeltaRecord)*((baseSize - BegSize - EndSize)/STRMIN + 10 )); 
	NewLink =(struct DeltaRecord *)malloc(sizeof(struct DeltaRecord)*((newSize - BegSize - EndSize)/STRMIN + 10 ));
	int Num1 = 0, Num2 = 0, DeltaLen = 0;
	//printf("Base:\n");
	Num1 = Chunking(baseBuf + BegSize, baseSize - BegSize - EndSize, &BaseLink[Beg]);
	//printf("Input:\n");
	Num2 = Chunking(newBuf + BegSize,	newSize - BegSize - EndSize,  &NewLink[Beg]);
	if(Beg){
		NewLink[0].DupFlag = 2;
		NewLink[0].nLength = BegSize;
		NewLink[0].nOffset = 0;
		NewLink[0].nSimID  = 0;
		NewLink[0].nHash   = weakHash(newBuf,BegSize);
		BaseLink[0].DupFlag = 2;
		BaseLink[0].nLength = BegSize;
		BaseLink[0].nOffset = 0;
		BaseLink[0].nHash	= weakHash(baseBuf,BegSize);
		assert(NewLink[0].nHash  == BaseLink[0].nHash);
		int i, j;
		for(i = Beg; i < Num2 + Beg; i++ ){
			NewLink[i].nOffset += BegSize;
		}
		for(j = Beg; j < Num1 + Beg; j++){
			BaseLink[j].nOffset += BegSize;
		}
	}
	if(End){
		NewLink[Num2+Beg].DupFlag = 2;
		NewLink[Num2+Beg].nLength = EndSize;
		NewLink[Num2+Beg].nOffset = newSize - EndSize;
		NewLink[Num2+Beg].nSimID  = Num1+Beg;
		NewLink[Num2+Beg].nHash   = weakHash(newBuf+newSize - EndSize,EndSize);
		BaseLink[Num1+Beg].DupFlag = 2;
		BaseLink[Num1+Beg].nLength = EndSize;
		BaseLink[Num1+Beg].nOffset = baseSize - EndSize;
		BaseLink[Num1+Beg].nHash   = weakHash(baseBuf+baseSize - EndSize,EndSize);
		assert(NewLink[Num2+Beg].nHash==BaseLink[Num1+Beg].nHash);
	}

	double sizetest = baseSize - BegSize - EndSize;

	int offset = (char*)&BaseLink[0].psNextSubCnk - (char*)&BaseLink[0];	/* what? */
	struct htable* psHTable = (struct htable*)malloc(sizeof(struct htable));
#if POST_DEDUP
	init(&psHTable, offset, 8, baseSize*2/(STRAVG+1));
#else
	init(&psHTable, offset, 8, 8*1024);
#endif

	int i, j;
	for(j = Beg; j < Num1 + Beg; j++){
		psHTable->insert(psHTable, (unsigned char*)&BaseLink[j].nHash, &BaseLink[j]);
	}

	for(i = Beg; i < Num2 + Beg; i++){
		struct DeltaRecord *psDupSubCnk = NULL;
		if(psDupSubCnk = (struct DeltaRecord *)psHTable->lookup(psHTable, 
							(unsigned char*)&NewLink[i].nHash)) {
			if(NewLink[i].nLength == psDupSubCnk->nLength && 
						memcmp(newBuf + NewLink[i].nOffset, baseBuf + psDupSubCnk->nOffset,
								NewLink[i].nLength) == 0) {
				nStage2 += NewLink[i].nLength;
				NewLink[i].DupFlag = 1;
				NewLink[i].nSimID  = (int)(psDupSubCnk-(&BaseLink[0]));
				BaseLink[NewLink[i].nSimID ].DupFlag = 1;
			}else{
				printf("Spooky Hash Error!!!!!!!!!!!!!!!!!!\n");
			}
		}
	}
	/***End Stage 2***/

	double tCode;
		
	for(i = 0; i < (Num2 + Beg + End); i++){
		if(NewLink[i].DupFlag >= 1){
			struct DeltaUnit1 record;
			set_flag(&record,0);
			set_length(&record,NewLink[i].nLength);
			record.nOffset = BaseLink[NewLink[i].nSimID].nOffset;
			
			while( NewLink[i+1].DupFlag >= 1 && (i+1) < (Num2 + Beg + End))
			{
				if(BaseLink[NewLink[i+1].nSimID].nOffset != 
					NewLink[i].nLength + BaseLink[NewLink[i].nSimID].nOffset )
					break;
				set_length(&record,get_length(&record)+NewLink[i+1].nLength);
				i++;
			}
			
			memcpy(deltaBuf + DeltaLen, &record, sizeof(struct DeltaUnit1));
			DeltaLen += sizeof(struct DeltaUnit1);
		}else{
		/* non-identical strings */
			//temp; actually, delta compression should be better

			/***** stage3 *****/
			int mBeg=0,mEnd=0;
			if(i>0 && NewLink[i-1].DupFlag == 1 && 
				NewLink[i-1].nSimID+1 < Num1+Beg+End ){
				int xx=0;
				int jj=NewLink[i].nOffset;
				int kk=BaseLink[NewLink[i-1].nSimID+1].nOffset;
				//may be error!!
				//while(newBuf[jj++]==baseBuf[kk++] && xx<=NewLink[i].nLength
				//	&& xx<=(baseSize-BaseLink[NewLink[i-1].nSimID+1].nOffset)){xx++;}
				//modified by lcg
				while( xx<NewLink[i].nLength &&
					 xx<(baseSize-BaseLink[NewLink[i-1].nSimID+1].nOffset) &&
					 newBuf[jj++]==baseBuf[kk++])
					 {xx++;}
				if(xx>0){
					mBeg = xx;
					nStage3 += xx;
					nStage3_beg += xx;
				}
			}
			
			if(((i + 1) < (Num2 + Beg + End)) && NewLink[i+1].DupFlag == 1 &&
				NewLink[i+1].nSimID > 0){
				
				int xx=0;
				int jj=NewLink[i].nOffset+NewLink[i].nLength;
				int kk=BaseLink[NewLink[i+1].nSimID].nOffset;

				while( xx<NewLink[i].nLength && 
					xx<BaseLink[NewLink[i+1].nSimID].nOffset &&
					newBuf[--jj]==baseBuf[--kk])
					{xx++;}
				if(xx>0){
					mEnd = xx;
					nStage3 += xx;
					nStage3_end += xx;
				}
			}
			if(NewLink[i].nLength < (mBeg + mEnd)){
				mBeg = NewLink[i].nLength - mEnd;
			}

			if(mBeg){
				struct DeltaUnit1 record;
				set_flag(&record,0);
				set_length(&record,mBeg);
				record.nOffset = BaseLink[NewLink[i-1].nSimID+1].nOffset;
				assert(memcmp(baseBuf + record.nOffset, newBuf + NewLink[i].nOffset,mBeg)==0);
				memcpy( deltaBuf + DeltaLen, &record, sizeof(struct DeltaUnit1) );
				DeltaLen += sizeof(struct DeltaUnit1);
			}
			if(NewLink[i].nLength > (mBeg + mEnd)){
				struct DeltaUnit2 record;
				set_flag(&record,1);
				set_length(&record, NewLink[i].nLength-mBeg-mEnd);
				//record.nOffset = NewLink[i].nOffset+mBeg;
				memcpy( deltaBuf + DeltaLen, &record, sizeof(struct DeltaUnit2) );
				DeltaLen += sizeof(struct DeltaUnit2);
				assert( get_length(&record)>0);			
				memcpy( deltaBuf + DeltaLen, newBuf + NewLink[i].nOffset + mBeg, get_length(&record));
				DeltaLen += get_length(&record);
			}

			if(mEnd){
				struct DeltaUnit1 record;
				set_flag(&record,0);
				set_length(&record,mEnd);
				record.nOffset = BaseLink[NewLink[i+1].nSimID].nOffset - mEnd;
				assert(memcmp(baseBuf + record.nOffset, newBuf + NewLink[i+1].nOffset - mEnd, 
														mEnd)==0);
				memcpy( deltaBuf + DeltaLen, &record, sizeof(struct DeltaUnit1) );
				DeltaLen += sizeof(struct DeltaUnit1);
			}
		}
	}

	/*
	if(psHTable){
		psHTable->destroy(psHTable);
	}
	
	if(psHTable2){
		psHTable2->destroy(psHTable2);
	}
	*/
	
	if(psHTable){
		free(psHTable->table);
		free(psHTable);
	}
	
	free(BaseLink);
	free(NewLink);

	*deltaSize = DeltaLen;
	return DeltaLen;
		
}

/* this version is which we finally use */
int dDelta_Decode( uint8_t* deltaBuf,u_int32_t deltaSize, 
						 uint8_t* baseBuf, u_int32_t baseSize,
						 uint8_t* outBuf,	u_int32_t *outSize)
{
    /* datalength is the cursor of outBuf, and readLength deltaBuf */
	int dataLength = 0, readLength = 0;

	while(1)
	{	
		u_int32_t flag = get_flag(deltaBuf+readLength);

		if(flag==0){
			struct DeltaUnit1 record;
			memcpy( &record, deltaBuf + readLength, sizeof(struct DeltaUnit1));
			readLength += sizeof(struct DeltaUnit1);
			
			memcpy( outBuf + dataLength, baseBuf + record.nOffset, get_length(&record));
			dataLength += get_length(&record);
		}else {
			struct DeltaUnit2 record;
			memcpy( &record, deltaBuf + readLength, sizeof(struct DeltaUnit2));
			readLength += sizeof(struct DeltaUnit2);
			
			memcpy( outBuf + dataLength, deltaBuf+ readLength, get_length(&record));
			readLength += get_length(&record);
			dataLength += get_length(&record);
		}
		
		if( readLength >= deltaSize){
			break;
		}

	}
	*outSize = dataLength;
	return dataLength;
}
 
 /*
 * evolved from eDelta_Encode_v2, the base chunks tentatively-growing number
 * of chunks, defined by BASE_***(e.g. 4,16,64,256). and we chunk a few chunks
 * in the input(defined by INPUT_TRY) to probe.(like a dare-to-die corps, because
 * we abandon them at last)
 */
 int eDelta_Encode_v3( uint8_t* newBuf, u_int32_t newSize,
						 uint8_t* baseBuf, u_int32_t baseSize,
						 uint8_t* deltaBuf, u_int32_t *deltaSize)
 {
	 /* detect the head and tail of one chunk */
	 int beg=0,end=0,begSize=0,endSize=0;
	 
	 while( begSize+7 < baseSize && begSize+7 < newSize ){ 
		 if( *(uint64_t *)(baseBuf+begSize)  == \
			 *(uint64_t *)(newBuf+begSize) ){
			 begSize+=8;
		 }
		 else
			 break;
	 }
	 while( begSize < baseSize && begSize < newSize ){
		 if(baseBuf[begSize] == newBuf[begSize]){
			 begSize++;
		 }
		 else
			 break;
	 }
 
	 if(begSize>16)
		 beg=1;
	 else
		 begSize=0;
 
	 while( endSize+7 < baseSize && endSize+7 < newSize ){ 
		 if( *(uint64_t *)(baseBuf+baseSize-endSize-8)	== \
			 *(uint64_t *)(newBuf+newSize-endSize-8) ){
			 endSize+=8;
		 }
		 else
			 break;
	 }
	 while( endSize < baseSize && endSize < newSize ){ 
		 if(baseBuf[baseSize-endSize-1] == newBuf[newSize-endSize-1]){
			 endSize++;
		 }
		 else
			 break;
	 }
 
	 if(begSize+endSize > newSize)
		 endSize=newSize-begSize;
 
	 if(endSize>16)
		 end=1;
	 else
		 endSize=0;
	 /* end of detect */
	 
	 if(begSize+endSize >= baseSize){
		 struct DeltaUnit1 record1;
		 struct DeltaUnit2 record2;
		 u_int32_t deltaLen =0;
		 if(beg){
			 set_flag(&record1, 0);
			 record1.nOffset=0;
			 set_length(&record1, begSize);
			 memcpy( deltaBuf + deltaLen, &record1, sizeof(struct DeltaUnit1) );
			 deltaLen += sizeof(struct DeltaUnit1);
		 }
		 if(newSize-begSize-endSize>0){
			 set_flag(&record2, 1);
			 set_length(&record2, newSize-begSize-endSize);
			 memcpy( deltaBuf + deltaLen, &record2, sizeof(struct DeltaUnit2) );
			 deltaLen += sizeof(struct DeltaUnit2);
			 memcpy( deltaBuf + deltaLen, newBuf+begSize, get_length(&record2) );
			 deltaLen += get_length(&record2);
		 }
		 if(end){
			 set_flag(&record1, 0);
			 record1.nOffset=baseSize-endSize;
			 set_length(&record1, endSize);
			 memcpy( deltaBuf + deltaLen, &record1, sizeof(struct DeltaUnit1) );
			 deltaLen += sizeof(struct DeltaUnit1);
		 }
 
		 *deltaSize=deltaLen;
		 return deltaLen;
	 }
	 
	 u_int32_t deltaLen = 0;
	 int cursor_base = begSize;
	 int cursor_input = begSize;
	 int cursor_input1 = 0;
	 int cursor_input2 = 0;
	 int input_last_chunk_beg = begSize;
	 int inputPos = begSize;
	 int length;
	 u_int64_t hash;
	 struct DeltaRecord *psDupSubCnk = NULL;
	 struct DeltaUnit1 record1;
	 struct DeltaUnit2 record2;
	 set_flag(&record1, 0);
	 set_flag(&record2, 1);
	 int flag=0;/* to represent the last record in the deltaBuf, 
	 1 for DeltaUnit1, 2 for DeltaUnit2 */
 
	 int numBase=0;/* the total number of chunks that the base has chunked */
	 int numBytes=0;/* the number of bytes that the base chunks once */
	 struct DeltaRecord *BaseLink =(struct DeltaRecord *)malloc( sizeof(struct DeltaRecord) \
		 * ( (baseSize-begSize-endSize)/STRMIN + 50 ) );
		 
	 int offset = (char*)&BaseLink[0].psNextSubCnk - (char*)&BaseLink[0];
	 struct htable* psHTable = (struct htable*)malloc(sizeof(struct htable));
#if POST_DEDUP
			  init(&psHTable, offset, 8, baseSize*2/(STRAVG+1));
#else
			  init(&psHTable, offset, 8, 8*1024);
#endif


	 //int chunk_length;
	 int flag_chunk=1;//to tell if basefile has been chunked to the end
	 int numBytes_old;
	 int numBytes_accu=0;//accumulated numBytes in one turn of chunking the base
	 int probe_match;//to tell which chunk for probing matches the some base
	 int flag_handle_probe;//to tell whether the probe chunks need to be handled
 
	 if(beg){
		 record1.nOffset=0;
		 set_length(&record1, begSize);
		 memcpy( deltaBuf + deltaLen, &record1, sizeof(struct DeltaUnit1) );
		 deltaLen += sizeof(struct DeltaUnit1);
		 flag=1;
	 }
 
#define BASE_BEGIN 5
#define BASE_EXPAND 2
#define BASE_STEP 3
#define INPUT_TRY 5
 
 /* if deltaLen > newSize * RECOMPRESS_THRESHOLD in the first round,we'll
 * go back to greedy.
 */
#define GO_BACK_TO_GREEDY 0
 
#define RECOMPRESS_THRESHOLD 0.2
 
 
	 struct DeltaRecord InputLink[INPUT_TRY];
	 //int test=0;
 
	 while( inputPos < newSize-endSize ){
		 if(flag_chunk){
			 if( cursor_input - input_last_chunk_beg >= numBytes_accu * 0.8 ){
				 numBytes_old=numBytes_accu;
				 numBytes_accu=0;
 
				 /* chunk a few chunks in the input first */
				 //Chunking_v3(newBuf+cursor_input, newSize-endSize-cursor_input, INPUT_TRY, InputLink);
				 flag_handle_probe=1;
				 probe_match=INPUT_TRY;
				 int chunk_number=BASE_BEGIN, i, j;
				 for(i=0; i<BASE_STEP; i++){
					 numBytes = Chunking_v3(baseBuf+cursor_base, baseSize-endSize-cursor_base, \
					  chunk_number, BaseLink+numBase);
					 
					 for(j = 0; j < chunk_number; j++){
						 if(BaseLink[numBase+j].nLength == 0){
							 flag_chunk=0;
							 break;
						 }
						 BaseLink[numBase+j].nOffset += cursor_base;
						 psHTable->insert(psHTable, (unsigned char*)&BaseLink[numBase+j].nHash, \
							 &BaseLink[numBase+j]);
					 }
 
					 cursor_base+=numBytes;
					 numBase+=chunk_number;
					 numBytes_accu+=numBytes;
 
					 chunk_number*=BASE_EXPAND;
					 if(i==0){
						 cursor_input1 = cursor_input;
						 int j;
						 for(j=0;j<INPUT_TRY; j++){
							 cursor_input2 = cursor_input1;
							 cursor_input1 = chunk_gear(newBuf+cursor_input2, \
								 newSize-cursor_input2-endSize)+cursor_input2;
							 InputLink[j].nLength = cursor_input1-cursor_input2;
							 InputLink[j].nHash = weakHash(newBuf+cursor_input2, \
								 InputLink[j].nLength);
							 if(psDupSubCnk=(struct DeltaRecord *)psHTable->lookup(psHTable, 
							 			(unsigned char*)&(InputLink[j].nHash))) {
								 probe_match=j;
								 goto lets_break;
							 }
						 }
					 }
					 else{
					 	 int j;
						 for(j=0; j<INPUT_TRY; j++){
							 if(psDupSubCnk=(struct DeltaRecord *)psHTable->lookup(psHTable, 
							 			(unsigned char*)&(InputLink[j].nHash))){
								 probe_match=j;
								 goto lets_break;
							 }
						 }
					 }
 
					 if(flag_chunk == 0)
 lets_break:					 
						 break;
	 
				 }
 
				 input_last_chunk_beg = ( cursor_input > (input_last_chunk_beg+numBytes_old) ? \
					 cursor_input : (input_last_chunk_beg+numBytes_old) ) ;
				 //test++;
			 }
		 }
 
#if 1	//to handle the chunks in input file for probing	
		 if(flag_handle_probe){
		 	 int i;
			 for(i=0; i<INPUT_TRY; i++){
 
				 length = InputLink[i].nLength;
				 cursor_input = length + inputPos;
 
				 if(i == probe_match){
					 flag_handle_probe=0;
					 goto match;
				 }
				 else{
					 if(flag==2){
						 /* continuous unique chunks only add unique bytes into the deltaBuf, 
						  * but not change the last DeltaUnit2, so the DeltaUnit2 should be 
						  * overwritten when flag=1 or at the end of the loop.
						  */
						 memcpy( deltaBuf + deltaLen, newBuf + inputPos, length);
						 deltaLen += length;
						 set_length(&record2, get_length(&record2)+length);
					 }else{
						 set_length(&record2, length);
					 
						 memcpy( deltaBuf + deltaLen, &record2, sizeof(struct DeltaUnit2) );
						 deltaLen += sizeof(struct DeltaUnit2);
 
						 memcpy( deltaBuf + deltaLen, newBuf + inputPos, length);
						 deltaLen += length;
 
						 flag=2;
					 }
 
					 inputPos=cursor_input;
				 }
			 }
 
			 flag_handle_probe=0;
		 }
#endif
 
		 cursor_input=chunk_gear(newBuf+inputPos,newSize-inputPos-endSize)+inputPos;
 
		 length=cursor_input-inputPos;
		 hash = weakHash(newBuf + inputPos, length);
 
		 /* lookup */
		 if( psDupSubCnk=(struct DeltaRecord *)psHTable->lookup(psHTable, (unsigned char*)&hash)){
			 //printf("inputPos: %d length: %d\n", inputPos, length);
 match:
			 if( length== psDupSubCnk->nLength && 
						 memcmp( newBuf + inputPos, 
								 baseBuf + psDupSubCnk->nOffset,
								 length)==0){
				 if(flag == 2){
					 /* continuous unique chunks only add unique bytes into the deltaBuf, 
					 * but not change the last DeltaUnit2, so the DeltaUnit2 should be 
					 * overwritten when flag=1 or at the end of the loop.
					 */
					 memcpy(deltaBuf+deltaLen-get_length(&record2)-sizeof(struct DeltaUnit2), 
						 &record2, sizeof(struct DeltaUnit2));
				 }
 
				 //greedily detect forward
				 int j=0;
 
				 while( psDupSubCnk->nOffset+length+j+7 < baseSize-endSize &&
					 cursor_input+j+7 < newSize-endSize ){ 
					 if( *(uint64_t *)(baseBuf+psDupSubCnk->nOffset+length+j)  == \
						 *(uint64_t *)(newBuf+cursor_input+j) ){
						 j+=8;
					 }
					 else
						 break;
				 }
				 while( psDupSubCnk->nOffset+length+j < baseSize-endSize && 
					 cursor_input+j < newSize-endSize ){ 
					 if(baseBuf[psDupSubCnk->nOffset+length+j] == newBuf[cursor_input+j]){
						 j++;
					 }
					 else
						 break;
				 }
 
				 cursor_input+=j;
				 if(psDupSubCnk->nOffset+length+j > cursor_base)
					 cursor_base = psDupSubCnk->nOffset+length+j;
 
				 set_length(&record1, cursor_input-inputPos);
				 record1.nOffset = psDupSubCnk->nOffset;
 
				 /* detect backward */
				 int k=0;
				 if(flag == 2){
					 while(k+1 <= psDupSubCnk->nOffset && k+1 <= get_length(&record2)){
						 if(baseBuf[psDupSubCnk->nOffset-(k+1)] == newBuf[inputPos-(k+1)])
							 k++;
						 else
							 break;
					 }
				 }
				 if(k>0){
					 deltaLen-=get_length(&record2);
					 deltaLen-=sizeof(struct DeltaUnit2);
 
					 set_length(&record2, get_length(&record2)-k);
 
					 if(get_length(&record2) > 0){
						 memcpy( deltaBuf + deltaLen, &record2, sizeof(struct DeltaUnit2) );
						 deltaLen += sizeof(struct DeltaUnit2);
						 deltaLen += get_length(&record2);
					 }
				 
					 set_length(&record1, get_length(&record1)+k);
					 record1.nOffset -= k; 
				 }
 
				 memcpy( deltaBuf + deltaLen, &record1, sizeof(struct DeltaUnit1) );
				 deltaLen += sizeof(struct DeltaUnit1);
				 flag=1;
			 }
			 else{
				 printf("Spooky Hash Error!!!!!!!!!!!!!!!!!!\n");
				 goto handle_hash_error;
			 }
		 }
		 else{
 handle_hash_error:
			 if(flag==2){
				 /* continuous unique chunks only add unique bytes into the deltaBuf, 
				  * but not change the last DeltaUnit2, so the DeltaUnit2 should be 
				  * overwritten when flag=1 or at the end of the loop.
				  */
				 memcpy( deltaBuf + deltaLen, newBuf + inputPos, length);
				 deltaLen += length;
				 set_length(&record2, get_length(&record2)+length);
			 }else{
				 set_length(&record2, length);
			 
				 memcpy(deltaBuf + deltaLen, &record2, sizeof(struct DeltaUnit2));
				 deltaLen += sizeof(struct DeltaUnit2);
 
				 memcpy(deltaBuf + deltaLen, newBuf + inputPos, length);
				 deltaLen += length;
 
				 flag=2;
			 }
		 }
 
		 inputPos=cursor_input;
		 //printf("cursor_input:%d\n",inputPos);
	 } 
 
	 if(flag == 2){
		 /* continuous unique chunks only add unique bytes into the deltaBuf, 
		 * but not change the last DeltaUnit2, so the DeltaUnit2 should be overwritten
		 * when flag=1 or at the end of the loop.
		 */
		 memcpy(deltaBuf + deltaLen - get_length(&record2) - sizeof(struct DeltaUnit2), 
			 &record2, sizeof(struct DeltaUnit2));
	 }
 
	 if(end){
		 record1.nOffset=baseSize-endSize;
		 set_length(&record1, endSize);
		 memcpy( deltaBuf + deltaLen, &record1, sizeof(struct DeltaUnit1) );
		 deltaLen += sizeof(struct DeltaUnit1);
	 }

	 /*
	 if(psHTable){
		 psHTable->destroy(psHTable);
	 }
	 */
	 
	 if(psHTable){
		free(psHTable->table);
		free(psHTable);
	}
	 
	 free(BaseLink);
 
	 *deltaSize=deltaLen;
	 return deltaLen;
 }
						 
/*
Compute the hash digest for the buf
*/
uint64_t weakHash(unsigned char *buf, int len) {
	uint64_t res;
#if HASH == 0
	res = spooky_hash64(buf, len, 0x1af1);
#elif HASH == 1
	res = XXH64(buf, len, 0x7fcaf1);
#endif
	return res;
}
