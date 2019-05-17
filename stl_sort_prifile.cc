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

string FILE_PATH = "sample_input.txt";


class Data{
public:
	uint32_t key;
	char value[4096]; // 교수님은 64bit로 하라고 하셨는데 그럼 string은 8글자고 너무 작은것같은데... ㅠ, 일단 4KB로 해보자
};

void *randstring(size_t length, char *buf) { // length should be qualified as const if you follow a rigorous standard
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!"; // could be const
	if (length) {
		if (buf) {
			int l = (int) (sizeof(charset) -1); // (static/global, could be const or #define SZ, would be even better)
			int key;  // one-time instantiation (static/global would be even better)
			for (int n = 0;n < length;n++) {        
				key = rand() % l;   // no instantiation, just assignment, no overhead from sizeof
				buf[n] = charset[key];
			}
			buf[length] = '\0';
		}
	}
}

void GenerateDataFile(string file_path){
	Data data;
	
	randstring(sizeof(data.value), data.value);
	data.key = rand() % UINT32_MAX;

	int fd = open( file_path.c_str(), O_RDWR | O_CREAT | O_LARGEFILE, S_IRUSR);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	

	

	
}
	
int main(void){
	
	GenerateDataFile(FILE_PATH);

}
