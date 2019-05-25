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
#define DATA_SIZE 128
#define MEM_SIZE ((int64_t)2*1024*1024*1024) // 2GB
#define NR_RUNS 10
#define INPUT_PATH "./input.txt"
#define RUNS_DIR_PATH "./runs/"
#define USE_EXISTING_DATA true

#define NR_ENTRIES_MEM (MEM_SIZE/DATA_SIZE)
#define NR_ENTRIES (NR_ENTRIES_MEM*NR_RUNS)
#define TOTAL_DATA_SIZE	DATA_SIZE*NR_ENTRIES


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

int64_t WriteData(int fd, char *buf, int64_t buf_size){
	int64_t size = 0;
	int len = 0;

	while(1){
		
		if((len = write(fd, &buf[0] + size , buf_size - size)) > 0){
			DBG_P("len: %d\n", len);
			
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				DBG_P("got EINTR\n");
				continue;	
			}
			else{
				return -1;
			}
		}
	}
}

int64_t ReadData(int fd, char *buf, int64_t buf_size){
	int64_t size = 0;
	int len = 0;

	while(1){
		
		if((len = read(fd, &buf[0] + size, buf_size - size)) > 0){
			DBG_P("len: %d\n", len);
			
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				DBG_P("got EINTR\n");
				continue;
			}
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
		int64_t tmp_byte_read = ReadData(fd_input, (char *)g_buffer, MEM_SIZE);
		if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
		}
		DBG_P("tmp_byte_read is: %zu\n", tmp_byte_read);

		nbyte_read += tmp_byte_read;
		DBG_P("nbyte_read: %ld\n", nbyte_read);

		/*q_sort*/
		sort(&g_buffer[0], &g_buffer[0] + NR_ENTRIES_MEM, compare);
		DBG_P("data sorted\n");

		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(run_idx++);

		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}

		WriteData(fd_run, (char *)g_buffer, MEM_SIZE);
		DBG_P("run %d is flushed\n", run_idx-1);
		
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
	printf("sizeof Data: %zu\n", sizeof(Data));

	// unaligned memory allocation
	// Data *tmp = new Data[NR_ENTRIES_MEM];
	
	// alignment 맞게 동적할당 하는 방법
	void *mem;
	posix_memalign(&mem, 4096, MEM_SIZE);
	Data *tmp = new (mem) Data;
	g_buffer = tmp;
	
#if !USE_EXISTING_DATA
	if(GenerateDataFile() != -1){
		DBG_P("Data file generated\n");
	}
#endif

	RunFormation();
	
	free(tmp);
	DBG_P("free tmp\n");

	return 0; 
}
