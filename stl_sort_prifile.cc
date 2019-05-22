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

// 교수님은 64bit로 하라고 하셨는데 그럼 string은 8글자고 너무 작은것같은데... ㅠ, 일단 512B로 해보자
#define VALUE_SIZE 64
//#define NR_ENTRIES 1000000
#define NR_ENTRIES 1000
#define NR_ENTRIES_MEM 10
#define INPUT_PATH "./input.txt"
#define RUNS_DIR_PATH "./runs/"

struct Data{
	uint32_t key;
	char value[VALUE_SIZE];
};

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

void GenerateDataFile(){
	Data data[NR_ENTRIES];

	srand((unsigned int)time(0));

	for(int i=0; i<NR_ENTRIES; i++){
		randstring(sizeof(data[i].value), data[i].value);
		data[i].key = rand() % UINT32_MAX;
		DBG_P("data[%d].key = %d\n", i,data[i].key);
	}

	int fd = open( INPUT_PATH, O_RDWR | O_CREAT | O_LARGEFILE, S_IRUSR);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file(%s) - %s\n", INPUT_PATH, strerror(errno));
	}

	write(fd, data, sizeof(data));
	close(fd);
}

void RunFormation(){
	Data data[NR_ENTRIES_MEM];

	int fd_input = open( INPUT_PATH, O_RDONLY);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}
	
	size_t nr_remain = NR_ENTRIES;
	size_t run_idx=0;

	while(nr_remain > 0){
		size_t byte_written = read(fd_input, data, sizeof(data));
		DBG_P("byte_written is: %zu\n", byte_written);
		
		nr_remain -= NR_ENTRIES_MEM;

		/*q_sort*/
		sort(&data[0], &data[0] + NR_ENTRIES_MEM, compare);
		DBG_P("data sorted\n");
		
		string run_path = RUNS_DIR_PATH;
		run_path += "run_" + to_string(run_idx++);
		
		int fd_run = open( run_path.c_str(), O_RDWR | O_CREAT | O_LARGEFILE, S_IRUSR);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}

		for(int i=0; i<NR_ENTRIES_MEM; i++){
			DBG_P("key is: %d\n", data[i].key);
		}

		write(fd_run, data, sizeof(data));
		close(fd_run);
	}
	close(fd_input);
}

int main(int argc, char* argv[]){

	DBG_P("size of Data: %zu\n", sizeof(struct Data));

	GenerateDataFile();
	
	RunFormation();

	return 0;
}
