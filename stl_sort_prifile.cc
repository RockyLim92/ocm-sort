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
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	write(fd, data, sizeof(data));
	close(fd);
}

void LoadData(){
	Data data[NR_ENTRIES_MEM];

	int fd = open( INPUT_PATH, O_RDONLY);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	size_t nr_remain = NR_ENTRIES;
	size_t nr_entries_written = 0;

	while(nr_entries_written > 0){
		size_t tmp_written = read(fd, data, sizeof(data));
		


		for(int i=0; i<NR_ENTRIES_MEM; i++){
			DBG_P("key is: %d\n", data[i].key);
		}

		/*q_sort*/
		sort(&data[0], &data[0] + NR_ENTRIES_MEM, compare);
		DBG_P("data sorted\n");

		for(int i=0; i<NR_ENTRIES_MEM; i++){
			DBG_P("key is: %d\n", data[i].key);
		}

	}


	close(fd);
}

int main(int argc, char* argv[]){

	DBG_P("size of Data: %zu\n", sizeof(struct Data));

	GenerateDataFile();
	LoadData();

	return 0;
}
