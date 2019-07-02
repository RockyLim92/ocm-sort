#define _LARGEFILE63_SOURCE

#include <iostream>
#include <fstream>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <algorithm>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <queue>
#include <tbb/parallel_sort.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/concurrent_priority_queue.h>
#include <atomic>

#include "profile.h"
#include "debug.h"
#include "threadpool.h"

using namespace std;


#define BLK 0		// hardcoding / fixed
#define MERGE 1		// hardcoding / fixed

#define INPUT_PATH "/mnt/test/input.txt"
#define OUTPUT_PATH "/mnt/test/output.txt"
#define RUNS_DIR_PATH "/mnt/test/runs/"

#define TOTAL_DATA_SIZE ((int64_t)32*1024*1024*1024) // (32GB)
#define MEM_SIZE ((int64_t)2*1024*1024*1024)	// (2GB)
#define DATA_SIZE (128)
#define NR_THREAD 4 // same as the number of buffer
#define BUFFER_SIZE (MEM_SIZE / NR_THREAD) // (run formation) mem buffer = run size
#define USE_EXISTING_DATA true
#define USE_EXISTING_RUNS false
#define IS_PRL_MERGE true
#define IS_TBB false
#define NR_THREAD_TBB 4

#define NR_RUNS ((TOTAL_DATA_SIZE / MEM_SIZE) * NR_THREAD)
#define BLK_SIZE ((MEM_SIZE / NR_RUNS) / 2)	// (merge) blk buffer
#define BLK_PER_RUN (BUFFER_SIZE / BLK_SIZE)	
#define NR_ENTRIES_BLK (BLK_SIZE / DATA_SIZE)
#define NR_ENTRIES_BUFFER (BUFFER_SIZE / DATA_SIZE)
#define NR_ENTRIES (TOTAL_DATA_SIZE / DATA_SIZE)

int64_t WriteData(int fd, char *buf, int64_t buf_size);
int64_t ReadData(int fd, char *buf, int64_t buf_size);
void* t_RunFormation(void *data);

struct Data{
	uint32_t key;
	char value[DATA_SIZE-sizeof(uint32_t)];
};

struct idx_Data{
	uint32_t idx;
	struct Data data;
	bool operator<(const idx_Data &cmp) const {
		return data.key < cmp.data.key;
	}
	bool operator>(const idx_Data &cmp) const {
		return data.key >= cmp.data.key;
	}
};


Data *g_buffer;
Data *blk_buffer;
Data *merge_buffer; 

atomic_int run_idx;

struct RunformationArgs{
	int fd_input;
	off_t st_offset;
	char * r_buffer;
	int64_t nbyte_load;
};


bool compare(struct Data a, struct Data b){
	return  (a.key < b.key);
}

bool idx_compare(struct idx_Data a, struct idx_Data b){
	return  (a.data.key < b.data.key);
}

void *randstring(size_t length, char *buf) {
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";
	if (length) {
		if (buf) {
			int l = (int) (sizeof(charset) -1);
			int key;
			for (int n = 0;n < length;n++) {        
				key = rand() % l;
				buf[n] = charset[key];
			}
			buf[length] = '\0';
		}
	}
}


int GenerateDataFile(){

	srand((unsigned int)time(0));

	int fd = open( INPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
	//int fd = open( INPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | 0644);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file(%s) - %s\n", INPUT_PATH, strerror(errno));
	}

	DBG_P("file opend\n");
	
	int64_t nbyte_buffer = 0;
	int64_t nbyte_written = 0;
	int64_t offset = 0;

	for(size_t i=0; i<NR_ENTRIES; i++){

		randstring(sizeof(g_buffer[offset].value), g_buffer[offset].value);
		g_buffer[offset].key = rand() % UINT32_MAX;

		offset++;
		nbyte_buffer += DATA_SIZE;

		if(nbyte_buffer == MEM_SIZE){
			DBG_P("nbyte_buffer: %ld\n", nbyte_buffer);

			//char *buffer__ = (char *)g_buffer;

			int64_t tmp_nbyte_written = WriteData(fd, (char *)g_buffer, nbyte_buffer);
			if(tmp_nbyte_written == -1){
				fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
				return -1;
			}
			DBG_P("tmp_nbyte_written: %ld\n", tmp_nbyte_written);
			
			nbyte_written += tmp_nbyte_written;
			nbyte_buffer = 0;
			offset = 1;

			DBG_P("g_buffer flushed, nr_entries: %zu\n", i+1);
		}
	}

	if(nbyte_written != TOTAL_DATA_SIZE){
		fprintf(stderr, "FAIL: nbyte_written != TOTAL_DATA_SIZE\n");
		return -1;
	}

	close(fd);
}

int RunFormation(){
	
	run_idx = 0;

	pthread_t p_thread[NR_THREAD];
	int thread_id[NR_THREAD];
	struct RunformationArgs runformation_args[NR_THREAD];

	int fd_input = open( INPUT_PATH, O_RDONLY | O_DIRECT);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}
	
	off_t cumulative_offset = 0;
	for(int i=0; i<NR_THREAD; i++){
		runformation_args[i].fd_input = fd_input;
		runformation_args[i].st_offset = cumulative_offset;
		runformation_args[i].r_buffer = (char *)g_buffer + cumulative_offset;
		runformation_args[i].nbyte_load = TOTAL_DATA_SIZE / NR_THREAD;
	

		thread_id[i] = pthread_create(&p_thread[i], NULL, t_RunFormation, (void*)&runformation_args[i]);
	
		cumulative_offset += BUFFER_SIZE;
	}
	
	int is_ok = 0;
	int res = 0;
	for(int i=0; i<NR_THREAD; i++){
		pthread_join(p_thread[i], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}

	close(fd_input);
	return res;
}

void* t_RunFormation(void *data){

	struct RunformationArgs args = *(struct RunformationArgs*)data;
	int fd_input = args.fd_input;
	char *r_buffer = args.r_buffer;
	Data *g_buffer__ = (Data *)r_buffer;
	int64_t nbyte_load = args.nbyte_load;

	int64_t nbyte_read = 0;

	while(nbyte_read < nbyte_load){
		
		/* === PROFILE START (load)=== */
        struct timespec local_time_load[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_load[0]);
        int64_t tmp_byte_read = ReadData(fd_input, r_buffer, BUFFER_SIZE);
        clock_gettime(CLOCK_MONOTONIC, &local_time_load[1]);
        calclock(local_time_load, &total_run_formation_load_time, &total_run_formation_load_count);
		/* === PROFILE END === */

        if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
			return ((void *)-1);
		}
		DBG_P("tmp_byte_read is: %zu\n", tmp_byte_read);

		nbyte_read += tmp_byte_read;
		DBG_P("nbyte_read: %ld\n", nbyte_read);

		
		/* === PROFILE START (sort) === */
        struct timespec local_time_sort[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_sort[0]);
#if IS_TBB
		tbb::parallel_sort(&g_buffer__[0], &g_buffer__[0] + NR_ENTRIES_BUFFER, compare);
#else
		sort(&g_buffer__[0], &g_buffer__[0] + NR_ENTRIES_BUFFER, compare);
#endif
		clock_gettime(CLOCK_MONOTONIC, &local_time_sort[1]);
        calclock(local_time_sort, &total_run_formation_sort_time, &total_run_formation_sort_count);
		/* === PROFILE END === */
        
		DBG_P("data sorted\n");

		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(run_idx++);

		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
			return ((void *)-1);
		}

		/* === PROFILE START (flush) === */
        struct timespec local_time_store[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_store[0]);
        int64_t tmp_byte_write = WriteData(fd_run, (char *)g_buffer__, BUFFER_SIZE);
        clock_gettime(CLOCK_MONOTONIC, &local_time_store[1]);
        calclock(local_time_store, &total_run_formation_store_time, &total_run_formation_store_count);
		/* === PROFILE END === */

        if(tmp_byte_write == -1){
			fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
			return ((void *)-1);
		}
		DBG_P("tmp_byte_write is: %zu\n", tmp_byte_read);
		

		DBG_P("run %d is done\n", run_idx-1);
		close(fd_run);
	}

	DBG_P("thread is done\n");
	return ((void *)1);
}

void Merge(){
	int run_count = NR_RUNS;
	int fd_run[run_count];
	bool idx_run[2] = { 0, };	// buffer index for sort, merge operations
	int64_t ptr_blk[run_count] = { 0, };
	int64_t ptr_run[run_count] = { 0, };
	blk_buffer = g_buffer;

	priority_queue<idx_Data, vector<idx_Data>, greater<idx_Data>> pq;
	ThreadPool::ThreadPool pool(NR_THREAD);

	int fd_output = open( OUTPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
	if(fd_output == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	for (int i = 0; i < run_count; i++){	// initialize blk buffer
		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(i);

		fd_run[i] = open( run_path.c_str(), O_RDONLY);
		if(fd_run[i] == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}
		pool.EnqueueJob(ReadData, fd_run[i], (char*)(blk_buffer + (i * NR_ENTRIES_BLK * 2)), (BLK_SIZE * 2));
		ptr_run[i]++;

		idx_Data tmp_buffer;
		tmp_buffer.data = blk_buffer[i * NR_ENTRIES_BLK * 2];
		tmp_buffer.idx = i;
		pq.push(tmp_buffer);

		ptr_blk[i]++;
	}

	int64_t nbyte_merged = 0;
	int64_t ptr_mrg = 0;
	while (nbyte_merged < TOTAL_DATA_SIZE){
		int pop_idx = pq.top().idx;
		merge_buffer[(NR_ENTRIES_BLK * idx_run[MERGE]) + ptr_mrg] = pq.top().data;
		pq.pop();
		if (ptr_mrg == NR_ENTRIES_BLK){
			DBG_P("merged key: %u\n", merge_buffer[NR_ENTRIES_BLK*idx_run[MERGE]].key);
#if IS_PRL_MERGE
			pool.EnqueueJob(WriteData, fd_output, (char *)(merge_buffer + NR_ENTRIES_BLK*idx_run[MERGE]), BLK_SIZE);		// backstage
#else
			WriteData(fd_output, (char *)(merge_buffer + NR_ENTRIES_BLK*idx_run[MERGE]), BLK_SIZE);
#endif
			nbyte_merged += BLK_SIZE;

			ptr_mrg = 0;
			idx_run[MERGE] = !idx_run[MERGE];	// toggle buffer index
			DBG_P("nbyte_merged: %ld\n", nbyte_merged);
		} else ptr_mrg++;

		if ((ptr_blk[pop_idx] == NR_ENTRIES_BLK) && (ptr_run[pop_idx] < BLK_PER_RUN)){  // run and block is empty
#if IS_PRL_MERGE
			pool.EnqueueJob(ReadData, fd_run[pop_idx], (char*)(blk_buffer + (pop_idx * NR_ENTRIES_BLK * 2) + (NR_ENTRIES_BLK * !idx_run[BLK])), BLK_SIZE);	// backstage
#else
			ReadData(fd_run[pop_idx], (char*)(blk_buffer + (pop_idx * NR_ENTRIES_BLK * 2) + (NR_ENTRIES_BLK * !idx_run[BLK])), BLK_SIZE);
#endif
			idx_run[BLK] = !idx_run[BLK];
			ptr_blk[pop_idx] = 0;
			ptr_run[pop_idx]++;
		}
		idx_Data tmp_buffer;
		tmp_buffer.data = blk_buffer[(pop_idx * NR_ENTRIES_BLK * 2) + (NR_ENTRIES_BLK * idx_run[BLK]) + ptr_blk[pop_idx]];
		tmp_buffer.idx = pop_idx;
		ptr_blk[pop_idx]++;

	//	pool.EnqueueJob(pq_push, pq, tmp_buffer);
		pq.push(tmp_buffer);
	}
	for (int i = 0; i < run_count; i++)
		close(fd_run[i]);
	close(fd_output);
}



void PrintStat(){
	printf("run_formation;load;sort;store\n");
	printf("%.2f;%.2f;%.2f;%.2f\n",(double)total_run_formation_time/1000000000,
					  (double)total_run_formation_load_time/1000000000,
					  (double)total_run_formation_sort_time/1000000000,
					  (double)total_run_formation_store_time/1000000000);
}

void PrintConfig(){
	printf("TOTAL_DATA_SIZE %zu\n", TOTAL_DATA_SIZE);
	printf("DATA_SIZE: %d\n", DATA_SIZE);
	printf("MEM_SIZE: %zu\n", MEM_SIZE);
	printf("NR_THREAD: %d\n", NR_THREAD);
	printf("NR_BUFFER: %d\n", NR_THREAD);
	printf("BUFFER_SIZE: %zu\n", BUFFER_SIZE);
	printf("NR_ENTRIES_BUFFER: %zu\n", NR_ENTRIES_BUFFER);
	printf("NR_ENTRIES: %zu\n", NR_ENTRIES);
}


int main(int argc, char* argv[]){

	int res = 0;
	
#if IS_TBB
	tbb::task_scheduler_init init(NR_THREAD_TBB);
#endif


	// aligned memory allocation
	void *mem;
	void *mem2;	// kh
	posix_memalign(&mem, 4096, MEM_SIZE);
	posix_memalign(&mem2, 4096, MEM_SIZE);
	Data *tmp = new (mem) Data;
	Data *tmp2 = new (mem2) Data;
	g_buffer = tmp;
	merge_buffer = tmp2;	

	PrintConfig();
	
#if !USE_EXISTING_DATA
	if(GenerateDataFile() != -1){
		DBG_P("Data file generated\n");
	}
#endif

#if !USE_EXISTING_RUNS
	/* === PROFILE START (run formation)=== */
	struct timespec local_time1[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time1[0]);
	res = RunFormation();
	clock_gettime(CLOCK_MONOTONIC, &local_time1[1]);
	calclock(local_time1, &total_run_formation_time, &total_run_formation_count);
	/* === PROFILE END === */
	if(res != 0){
		fprintf(stderr, "FAIL: run formation\n");
		return -1;
	}
#endif

	/* merge */
        struct timeval diff, startTV, endTV;
        gettimeofday(&startTV, NULL);
        Merge();        //      kh
        gettimeofday(&endTV, NULL);
        timersub(&endTV, &startTV, &diff);
        printf("*time taken for merge = %ld %ld \n", diff.tv_sec, diff.tv_usec);
	
	PrintStat();
	free(tmp);
	return 0; 
}
