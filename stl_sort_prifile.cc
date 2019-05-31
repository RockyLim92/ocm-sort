#define _LARGEFILE63_SOURCE

#include <iostream>
#include <sstream>	// kh
#include <fstream>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <queue>	// kh
#include <vector>	// kh
#include "tbb/parallel_sort.h"	// kh
#include "tbb/task_scheduler_init.h"	// kh
#include "profile.h"
#include "debug.h"


using namespace std;

#define DATA_SIZE (128)
#define MEM_SIZE ((int64_t)2*1024*1024*1024) // 2GB
#define NR_RUNS 8
#define INPUT_PATH "./input.txt"
#define OUTPUT_PATH "./output.txt"	// kh
#define RUNS_DIR_PATH "./runs/"
#define USE_EXISTING_DATA true

#define BLK_SIZE (MEM_SIZE/NR_RUNS)	// kh
#define BLK_PER_RUN (MEM_SIZE/BLK_SIZE)	// kh
#define NR_ENTRIES_BLK (BLK_SIZE/DATA_SIZE) //	kh
#define NR_ENTRIES_MEM (MEM_SIZE/DATA_SIZE)
#define NR_ENTRIES (NR_ENTRIES_MEM*NR_RUNS)
#define TOTAL_DATA_SIZE	DATA_SIZE*NR_ENTRIES


int64_t WriteData(int fd, char *buf, int64_t buf_size);
int64_t ReadData(int fd, char *buf, int64_t buf_size);



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
Data *blk_buffer;	// kh
Data *merge_buffer;	// kh

bool compare(struct Data a, struct Data b){
	return  (a.key < b.key);
}


bool idx_compare(struct idx_Data a, struct idx_Data b){	// 	kh
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

void RunFormation(){

	int fd_input = open( INPUT_PATH, O_RDONLY);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	int64_t nbyte_read = 0;
	int run_idx=0;

	while(nbyte_read < TOTAL_DATA_SIZE){
		

        struct timespec local_time_load[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_load[0]);
        // load
        int64_t tmp_byte_read = ReadData(fd_input, (char *)g_buffer, MEM_SIZE);
        clock_gettime(CLOCK_MONOTONIC, &local_time_load[1]);
        calclock(local_time_load, &total_run_formation_load_time, &total_run_formation_load_count);
        
        
        if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
		}
		DBG_P("tmp_byte_read is: %zu\n", tmp_byte_read);

		nbyte_read += tmp_byte_read;
		DBG_P("nbyte_read: %ld\n", nbyte_read);

		
        struct timespec local_time_sort[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_sort[0]);
        /*q_sort*/
	//	sort(&g_buffer[0], &g_buffer[0] + NR_ENTRIES_MEM, compare);
        	tbb::parallel_sort(g_buffer, g_buffer + NR_ENTRIES_MEM, compare);
	clock_gettime(CLOCK_MONOTONIC, &local_time_sort[1]);
        calclock(local_time_sort, &total_run_formation_sort_time, &total_run_formation_sort_count);
		
        
        DBG_P("data sorted\n");

		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(run_idx++);

		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}

        struct timespec local_time_store[2];
        clock_gettime(CLOCK_MONOTONIC, &local_time_store[0]);
		// store
        WriteData(fd_run, (char *)g_buffer, MEM_SIZE);
        clock_gettime(CLOCK_MONOTONIC, &local_time_store[1]);
        calclock(local_time_store, &total_run_formation_store_time, &total_run_formation_store_count);


		DBG_P("run %d is flushed\n", run_idx-1);
		
		close(fd_run);
	}
	close(fd_input);
}

void Merge(){
	int run_count = NR_RUNS;
	int fd_run[run_count];
	int64_t ptr_blk[run_count] = { 0, };
	int64_t ptr_run[run_count] = { 0, };
	blk_buffer = g_buffer;
	priority_queue<idx_Data, vector<idx_Data>, greater<idx_Data>> pq;

	for (int i = 0; i < run_count; i++){
		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(i);

		fd_run[i] = open( run_path.c_str(), O_RDONLY);
		if(fd_run[i] == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}
		ReadData(fd_run[i], (char*)(blk_buffer + i * NR_ENTRIES_BLK), BLK_SIZE);
		ptr_run[i]++;
		
		idx_Data tmp_buffer;
		tmp_buffer.data = blk_buffer[i * NR_ENTRIES_BLK];
		tmp_buffer.idx = i;
		pq.push(tmp_buffer);
		
		ptr_blk[i]++;
	}

	int fd_output = open( OUTPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
	if(fd_output == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}
	
	int64_t nbyte_merged = 0;
	int64_t ptr_mrg = 0;
	while (nbyte_merged < TOTAL_DATA_SIZE){
		int pop_idx = pq.top().idx;
		merge_buffer[ptr_mrg] = pq.top().data;
		pq.pop();

		if (ptr_mrg == NR_ENTRIES_BLK){		// full merge buffer
			DBG_P("merged key: %u\n", merge_buffer[0].key);	
			int64_t tmp_nbyte_merged = WriteData(fd_output, (char *)merge_buffer, BLK_SIZE);
			if(tmp_nbyte_merged == -1){
				fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
			} else nbyte_merged += tmp_nbyte_merged;
			ptr_mrg = 0;
//			DBG_P("nbyte_merged: %ld\n", nbyte_merged);
		} else ptr_mrg++;

		if ((ptr_blk[pop_idx] == NR_ENTRIES_BLK) && (ptr_run[pop_idx] < BLK_PER_RUN)){	// run, block empty
			ReadData(fd_run[pop_idx], (char*)(blk_buffer + pop_idx*NR_ENTRIES_BLK), BLK_SIZE);
//			DBG_P("blk_%lu/%lu of run_%d loaded\n", ptr_run[pop_idx], BLK_PER_RUN - 1, pop_idx);
			ptr_blk[pop_idx] = 0;
			ptr_run[pop_idx]++;
		}

		idx_Data tmp_buffer;
		tmp_buffer.data = blk_buffer[(pop_idx * NR_ENTRIES_BLK) + ptr_blk[pop_idx]];
		tmp_buffer.idx = pop_idx;
		ptr_blk[pop_idx]++;
		pq.push(tmp_buffer);
	}
	
	for (int i = 0; i < run_count; i++)
		close(fd_run[i]);
	close(fd_output);




}

void PrintStat(){
    printf("runformation - total time: %llu, total_cout: %llu\n", total_run_formation_time, total_run_formation_count);
    printf("runformation - load time: %llu, load_cout: %llu\n", total_run_formation_load_time, total_run_formation_load_count);
    printf("runformation - sort time: %llu, sort_cout: %llu\n", total_run_formation_sort_time, total_run_formation_sort_count);
    printf("runformation - store time: %llu, load_cout: %llu\n", total_run_formation_store_time, total_run_formation_store_count);
}

void PrintConfig(){
	printf("VALUESIZE: %d\n", DATA_SIZE);
	printf("MEM_SIZE: %zu\n", MEM_SIZE);
	printf("NR_RUNS: %d\n", NR_RUNS);
	printf("NR_ENTRIES_MEM: %zu\n", NR_ENTRIES_MEM);
	printf("NR_ENTRIES: %zu\n", NR_ENTRIES);
	printf("TOTAL_DATA_SIZE %zu\n", TOTAL_DATA_SIZE);
	printf("sizeof Data: %zu\n", sizeof(Data));
}

int main(int argc, char* argv[]){

	tbb::task_scheduler_init init();	// kh
	PrintConfig();
	
	// unaligned memory allocation
	// Data *tmp = new Data[NR_ENTRIES_MEM];
	
	// aligned memory allocation
	void *mem;
	void *mem2;
	posix_memalign(&mem, 4096, MEM_SIZE);
	posix_memalign(&mem2, 4096, MEM_SIZE);	//	kh
	Data *tmp = new (mem) Data;
	Data *tmp2 = new (mem2) Data;	//	kh
	g_buffer = tmp;
	merge_buffer = tmp2;	//	kh
	
#if !USE_EXISTING_DATA
	if(GenerateDataFile() != -1){
		DBG_P("Data file generated\n");
	}
#endif

	struct timespec local_time1[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time1[0]);
	/* run formation */
//	RunFormation();
	clock_gettime(CLOCK_MONOTONIC, &local_time1[1]);
	calclock(local_time1, &total_run_formation_time, &total_run_formation_count);

	/* merge */	
	Merge();	//	kh

	PrintStat();

	free(tmp);
	return 0; 
}
