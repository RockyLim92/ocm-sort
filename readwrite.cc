#include <stdint.h>
#include "debug.h"
#include <errno.h>
#include <unistd.h>
#include <pthread.h>

int64_t ParallelWriteData(int fd, char *buf, int64_t buf_size, int nr_threads){

	pthread_t *p_thread = new pthread_t[nr_threads];
	int *thread_id = new int[nr_threads];

	for(int i=0; i<nr_threads; i++){
		*thread_id = pthread_create(&p_thread[i], );
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
