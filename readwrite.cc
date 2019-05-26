#include <stdint.h>
#include "debug.h"
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <cstdio>

struct ReadWriteArgs{
	int fd;
	char *buf;
	int64_t buf_size;
};

void *t_WriteData(void *data);
void *t_ReadData(void *data);


void *t_WriteData(void *data){

	struct ReadWriteArgs rwas = *(struct ReadWriteArgs*)data;

	int fd = rwas.fd;
	char *buf = rwas.buf;
	int64_t buf_size = rwas.buf_size;

	int64_t size = 0;
	int len = 0;

	while(1){
		
		if((len = write(fd, &buf[0] + size , buf_size - size)) > 0){
			DBG_P("len: %d\n", len);
			
			size += len;
			if(size == buf_size){
				return (void*)size;
			}
		}
		else if(len == 0){
			return (void *)size;
		}
		else{
			if(errno == EINTR){
				DBG_P("got EINTR\n");
				continue;	
			}
			else{
				return (void *)-1;
			}
		}
	}
}

int64_t ParallelWriteData(int fd, char *buf, int64_t buf_size, int nr_threads){

	pthread_t *p_thread = new pthread_t[nr_threads];
	int *thread_id = new int[nr_threads];
	
	struct ReadWriteArgs **rwas = new struct ReadWriteArgs*[nr_threads];

	for(int i=0; i<nr_threads; i++){
		rwas[i] = new struct ReadWriteArgs;
		rwas[i]->fd = fd;
		
		// TODO 스레드 수에 따라 buf주소 및 buf_size 적당하게 나누어주기
		//rwas[i]->buf = buf;
		//rwas[i]->buf_size = buf_size;
		
		*thread_id = pthread_create(&p_thread[i], NULL,t_WriteData, (void*)rwas[i]);
	}
	
	int64_t total_nbyte_written = 0;
	int64_t tmp_nbyte_written;
	
	for(int i=0; i<nr_threads; i++){
		pthread_join(p_thread[i], (void **)&tmp_nbyte_written);
		total_nbyte_written += tmp_nbyte_written;	
	}

	for(int i=0; i<nr_threads; i++){
		delete rwas[i];
	}
	delete[] rwas;
	delete[] p_thread;
	delete[] thread_id;
	
	// 잘 읽었는지 검사하기
	if(total_nbyte_written != buf_size){
		fprintf(stderr, "total_nbyte_written != buf_size\n");
		return -1;
	}
	DBG_P("total_nbyte_written: %ld\n", total_nbyte_written);
	

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
