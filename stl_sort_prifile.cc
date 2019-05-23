#define _LARGEFILE63_SOURCE

#include <iostream>
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

#include "profile.h"
#include "debug.h"

using namespace std;

// 교수님은 8byte로 하라고 하셨지만, 일단 64Byte로 해보자
#define DATA_SIZE 64
#define MEM_SIZE ((uint64_t)4*1024*1024*1024) // 4GB
#define NR_RUNS 10
#define INPUT_PATH "./input.txt"
#define RUNS_DIR_PATH "./runs/"

#define NR_ENTRIES_MEM ((uint64_t)MEM_SIZE/DATA_SIZE)
#define NR_ENTRIES ((uint64_t)NR_ENTRIES_MEM*NR_RUNS)
#define TOTAL_DATA_SIZE 64*NR_ENTRIES
#define BUFFER_SIZE NR_ENTRIES_MEM*DATA_SIZE


struct Data{
	uint32_t key;
	char value[DATA_SIZE-sizeof(uint32_t)];
};

Data *g_buffer;

bool compare(struct Data a, struct Data b){
	return  (a.key < b.key);
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


size_t WriteData(int fd, Data *buf, size_t buf_size){
	size_t size = 0;
	size_t len = 0;

	while(1){
		DBG_P("len = %zu\n", len);
		DBG_P("size = %zu\n", size);
		DBG_P("buf_size = %zu\n", buf_size);
		
		if((len = write(fd, &buf[size], buf_size - size)) > 0){
			size += len;
			if(size == buf_size){
				DBG_P("return size = %zu\n", size);
				return size;
			}
		}
		else if(len == 0){
			DBG_P("return size = %zu\n", size);
			return size;
		}
		else{
			if(errno == EINTR)
				continue;
			else
				return -1;
		}
	}
}

size_t ReadData(int fd, Data *buf, size_t buf_size){
	size_t size = 0;
	size_t len = 0;

	while(1){
		DBG_P("len = %zu\n", len);
		DBG_P("size = %zu\n", size);
		DBG_P("buf_size = %zu\n", buf_size);
		
		if((len = read(fd, &buf[size], buf_size - size)) > 0){
			size += len;
			if(size == buf_size){
				DBG_P("return size = %zu\n", size);
				return size;
			}
		}
		else if(len == 0){
			DBG_P("return size = %zu\n", size);
			return size;
		}
		else{
			if(errno == EINTR)
				continue;
			else
				return -1;
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

	//const size_t alignment_limit = (BUFFER_SIZE / 4096) * 4096;
	size_t nbyte_buffer = 0;
	size_t nbyte_written = 0;
	size_t nbyte_remain = MEM_SIZE;
	size_t offset = 0;

	for(size_t i=0; i<NR_ENTRIES; i++){

		randstring(sizeof(g_buffer[offset].value), g_buffer[offset].value);
		g_buffer[offset].key = rand() % UINT32_MAX;

		offset++;
		nbyte_buffer += DATA_SIZE;

		if(nbyte_buffer == MEM_SIZE){
			DBG_P("nbyte_buffer: %zu\n", nbyte_buffer);

			size_t tmp_nbyte_written = WriteData(fd, g_buffer, nbyte_buffer);
			if(tmp_nbyte_written == -1){
				fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
				return -1;
			}
			DBG_P("tmp_nbyte_written: %zu\n", tmp_nbyte_written);
			
			nbyte_written += tmp_nbyte_written;
			nbyte_buffer = 0;
			offset = 0;

			DBG_P("g_buffer flushed, nr_entries: %zu\n", i);
		}
	}

	if(nbyte_written != TOTAL_DATA_SIZE){
		fprintf(stderr, "FAIL: nbyte_written != TOTAO_DATA_SIZE\n");
		return -1;
	}
	close(fd);
}

void RunFormation(){

	int fd_input = open( INPUT_PATH, O_RDONLY);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	size_t nr_remain = NR_ENTRIES;
	size_t run_idx=0;

	while(nr_remain > 0){
		size_t tmp_byte_read = read(fd_input, g_buffer, MEM_SIZE);
		if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
		}
		DBG_P("tmp_byte_read is: %zu\n", tmp_byte_read);

		nr_remain -= NR_ENTRIES_MEM;
		DBG_P("remain: %zu\n", nr_remain);

		/*q_sort*/
		sort(&g_buffer[0], &g_buffer[0] + NR_ENTRIES_MEM, compare);
		DBG_P("data sorted\n");

		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(run_idx++);

		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}

		write(fd_run, g_buffer, sizeof(g_buffer));
		close(fd_run);
	}
	close(fd_input);
}

int main(int argc, char* argv[]){

	printf("VALUESIZE: %d\n", DATA_SIZE);
	printf("MEM_SIZE: %zu\n", MEM_SIZE);
	printf("NR_RUNS: %d\n", NR_RUNS);
	printf("NR_ENTRIES_MEM: %zu\n", NR_ENTRIES_MEM);
	printf("NR_ENTRIES: %zu\n", NR_ENTRIES);
	printf("TOTAL_DATA_SIZE %zu\n", TOTAL_DATA_SIZE);
	printf("BUFFER_SIZE %zu\n", BUFFER_SIZE);
	printf("sizeof Data: %zu\n", sizeof(Data));

	//Data *tmp = new Data[NR_ENTRIES_MEM];
	
	// alignment 맞게 동적할당 하는 방법
	void *mem;
	posix_memalign(&mem, 4096, BUFFER_SIZE);
	Data *tmp = new (mem) Data;
	g_buffer = tmp;

	if(GenerateDataFile() != -1){
		DBG_P("Data file generated\n");
	}

	RunFormation();

	delete[] g_buffer;

	return 0;
}
