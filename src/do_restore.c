#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "restore.h"
#include "xdelta/xdelta3.h"

struct container_restore {
    containerid cid;
};

static void* lru_restore_thread(void *arg) {

	struct lruCache *cache;
	cache = new_lru_cache(destor.restore_cache[1], free_container,
			lookup_fingerprint_in_container);

    /* record the restore information */
    sds fname = sdsdup(destor.working_directory);
    fname = sdscat(fname, "restoreRecord/bv");
	char s[20];
	sprintf(s, "%d", jcr.id);
	fname = sdscat(fname, s);
	fname = sdscat(fname, ".restore");

    sds fcid = sdsdup(destor.working_directory);
    fcid = sdscat(fcid, "restoreRecord/bv");
	fcid = sdscat(fcid, s);
	fcid = sdscat(fcid, ".cid");

    FILE* fp_restore = fopen(fname, "w");
    FILE* fp_cid = fopen(fcid, "w");

    if (!fp_restore || !fp_cid) {
		fprintf(stderr, "Can not create restoreRecord files");
		perror("The reason is");
		exit(1);
	}

    sdsfree(fname);
    sdsfree(fcid);
    /* end of record */

	struct chunk* c;
	while ((c = sync_queue_pop(restore_recipe_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(restore_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

        struct container *con = check_lru_container_list(cache, c->id);
		if (!con) {
            jcr.read_normal_chunks++;
			VERBOSE("Restore cache: container %lld is missed", c->id);
			con = load_container_by_id(c->id);
			lru_cache_insert(cache, con, NULL, NULL);

            jcr.read_container_num++;
		}
        else {
            jcr.hit_normal_chunks++;
            con = lru_cache_lookup(cache, &c->fp);
        }

        assert(con);

        struct chunk *rc = NULL;
        rc = get_chunk_in_container(con, &c->fp);

        if(c->size != rc->size || rc->size == 0) {
                printf("expect size %d, but the real size %d, delta chunk: %s\n",
                                        c->size, rc->size, rc->delta ? "yes" : "no");
		        assert(c->size == rc->size);
                assert(rc->size != 0);
	        }

		TIMER_END(1, jcr.read_chunk_time);

		if(rc->delta){
			/* It is a delta, decode it */

            struct container *base_con = check_lru_container_list(cache, rc->delta->base_id);
			if (!base_con) {
                jcr.read_base_chunks++;
				VERBOSE("Restore cache: the base container %lld is missed", rc->delta->base_id);
				base_con = load_container_by_id(rc->delta->base_id);
				lru_cache_insert(cache, base_con, NULL, NULL);

				jcr.read_container_num++;
			}
            else {
                jcr.hit_base_chunks++;
                base_con = lru_cache_lookup(cache, &rc->delta->base_fp);
            }

            assert(base_con);

            struct chunk *base_chunk = get_base_chunk_in_container(base_con, &rc->delta->base_fp);
		    assert(base_chunk);

            u_int32_t rsize;
	        unsigned char restoreBuf[4096*64];

			int res_decode = 0;

			switch(destor.delta_compression_method){
				case XDELTA:{
					res_decode = xd3_decode_memory( (uint8_t*)rc->delta->data, (usize_t)rc->delta->size,
							(uint8_t*)base_chunk->data, (usize_t)base_chunk->size,
							(uint8_t*)restoreBuf, (usize_t*)&rsize, (usize_t)rc->size, 1);

	        		if(rsize != rc->size) {
                		WARNING("decode xdelta, expect size %d, but the real size %d", rc->size, rsize);
						assert(rsize == rc->size);
	        		}
					break;
				}
				case DDELTA:{

					res_decode = dDelta_Decode( (uint8_t*)rc->delta->data, (usize_t)rc->delta->size,
							(uint8_t*)base_chunk->data, base_chunk->size,
							(uint8_t*)restoreBuf, &rsize);

					if(rsize != rc->size) {
						WARNING("decode Ddelta, expect size %d, but the real size %d", rc->size, rsize);
						assert(rsize == rc->size);
					}
					break;
				}
				case EDELTA:{
					res_decode = dDelta_Decode( (uint8_t*)rc->delta->data, (usize_t)rc->delta->size,
							(uint8_t*)base_chunk->data, base_chunk->size,
							(uint8_t*)restoreBuf, &rsize);

					if(rsize != rc->size) {
						WARNING("decode Edelta, expect size %d, but the real size %d", rc->size, rsize);
						assert(rsize == rc->size);
					}
					break;
				}
			}

            memcpy(rc->data, restoreBuf, rc->size);

            free_delta(rc->delta);
            rc->delta = NULL;
		}

        jcr.data_size += rc->size;
        sync_queue_push(restore_chunk_queue, rc);

		free_chunk(c);
	}

	sync_queue_term(restore_chunk_queue);

	free_lru_cache(cache);

    fclose(fp_restore);
    fclose(fp_cid);
	return NULL;
}

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		struct recipeMeta *r = read_next_recipe_meta(jcr.bv);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(restore_recipe_queue, c);

		jcr.file_num++;

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);

			struct chunk* c = new_chunk(0);
			memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);
			jcr.chunk_num++;

			sync_queue_push(restore_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(restore_recipe_queue, c);

		free_recipe_meta(r);
	}

	sync_queue_term(restore_recipe_queue);
	return NULL;
}

void write_restore_data() {

	char *p, *q;
	q = jcr.path + 1;/* ignore the first char*/
	/*
	 * recursively make directory
	 */
	while ((p = strchr(q, '/'))) {
		if (*p == *(p - 1)) {
			q++;
			continue;
		}
		*p = 0;
		if (access(jcr.path, 0) != 0) {
			mkdir(jcr.path, S_IRWXU | S_IRWXG | S_IRWXO);
		}
		*p = '/';
		q = p + 1;
	}

	struct chunk *c = NULL;
	FILE *fp = NULL;

	while ((c = sync_queue_pop(restore_chunk_queue))) {

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
			//NOTICE("Restoring: %s", c->data);

			sds filepath = sdsdup(jcr.path);
			filepath = sdscat(filepath, c->data);

			int len = sdslen(jcr.path);
			char *q = filepath + len;
			char *p;
			while ((p = strchr(q, '/'))) {
				if (*p == *(p - 1)) {
					q++;
					continue;
				}
				*p = 0;
				if (access(filepath, 0) != 0) {
					mkdir(filepath, S_IRWXU | S_IRWXG | S_IRWXO);
				}
				*p = '/';
				q = p + 1;
			}

			if (destor.simulation_level == SIMULATION_NO) {
				assert(fp == NULL);
				fp = fopen(filepath, "w");
			}

			sdsfree(filepath);

		} 
		else if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
			if (destor.simulation_level == SIMULATION_NO)
				fclose(fp);
			fp = NULL;
		} 
		else {
			if(destor.simulation_level == SIMULATION_NO){
				VERBOSE("Restoring %d bytes", c->size);
				fwrite(c->data, c->size, 1, fp);
			}
		}

		free_chunk(c);

		TIMER_END(1, jcr.write_chunk_time);

		jcr.status = JCR_STATUS_DONE;
	}

}

void do_restore(int revision, char *path) {

	init_recipe_store();
	init_container_store();

	init_restore_jcr(revision, path);

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "restore to: %s", jcr.path);

	restore_chunk_queue = sync_queue_new(100);
	restore_recipe_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== restore begin ====");

	jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t;
	pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
	pthread_create(&read_t, NULL, lru_restore_thread, NULL);

	write_restore_data();

	do{
       usleep(500);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);

	assert(sync_queue_size(restore_chunk_queue) == 0);
	assert(sync_queue_size(restore_recipe_queue) == 0);

	free_backup_version(jcr.bv);

	TIMER_END(1, jcr.total_time);
	puts("==== restore end ====");

	printf("job id: %d\n", jcr.id);
	printf("restore path: %s\n", jcr.path);
	printf("number of files: %d\n", jcr.file_num);
	printf("number of chunks: %d\n", jcr.chunk_num);
	printf("total size(B): %ld\n", jcr.data_size);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.6f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("write_chunk_time : %.3fs, %.2fMB/s\n",
			jcr.write_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_chunk_time / 1024 / 1024);

	char logfile[] = "restore.log";
	FILE *fp = fopen(logfile, "a");

	/*
	 * job id,
	 * chunk num,
	 * data size,
	 * actually read container number,
	 * speed factor,
	 * throughput
	 */
	fprintf(fp, "%d %lld %d %.4f %.4f\n", jcr.id, jcr.data_size,
			jcr.read_container_num,
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
			jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);

	close_container_store();
	close_recipe_store();

    FILE *excel_restore = fopen("excel_restore.txt", "a");
    fprintf(excel_restore, "%d\t%lld\t%.4f\t%.2f\n", jcr.id + 1, 
		jcr.read_container_num,
		jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
		jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
    fclose(excel_restore);

	/*
    FILE *excel_restoration_process = fopen("excel_restoration_process.txt", "a");
    fprintf(excel_restoration_process, "%d\t%lld\t%lld\t%lld\t%lld\t%.4f\t%.4f\t%.4f\t%.4f\n", jcr.id + 1,
                        jcr.read_normal_chunks, jcr.hit_normal_chunks, jcr.read_base_chunks, jcr.hit_base_chunks,
                        1.0*jcr.hit_normal_chunks/(jcr.read_normal_chunks+jcr.hit_normal_chunks),
                        ((jcr.read_base_chunks+jcr.hit_base_chunks)==0) ? 0 : (1.0*jcr.hit_base_chunks/(jcr.read_base_chunks+jcr.hit_base_chunks)),
                        1.0*jcr.read_normal_chunks/jcr.read_container_num,
                        1.0*jcr.read_base_chunks/jcr.read_container_num);
    fclose(excel_restoration_process);
	*/
	
    FILE *excel_base_sparse = fopen("excel_percent_read_by_sparse_base.txt", "a");
    fprintf(excel_base_sparse, "%d\t%lld\t%lld\t%.4f\n", jcr.id + 1,
                        jcr.hit_sparse_base_cid,
                        jcr.miss_sparse_base_cid,
                        ((jcr.hit_sparse_base_cid+jcr.miss_sparse_base_cid)==0) ? 0 : (1.0*jcr.hit_sparse_base_cid/(jcr.hit_sparse_base_cid+jcr.miss_sparse_base_cid)));
    fclose(excel_base_sparse);
	/*
	FILE *excel_restore_throughput = fopen("excel_restore_statis.txt", "a");
    fprintf(excel_restore_statis, "%d\t%lld\t%d\t%.4f\t%.4f\n", jcr.id, jcr.data_size,
			jcr.read_container_num,
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num),
			jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));
    fclose(excel_restore_statis);
    */
}
