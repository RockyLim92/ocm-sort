#ifndef _PROFILE_H_
#define _PROFILE_H_

//function level profiling
#define BILLION     (1000000001ULL)
//#define calclock(timevalue, total_time, total_count, delay_time) do { 
#define calclock(timevalue, total_time, total_count) do { \
	unsigned long long timedelay, temp, temp_n; \
	struct timespec *myclock = (struct timespec*)timevalue; \
	if(myclock[1].tv_nsec >= myclock[0].tv_nsec){ \
		temp = myclock[1].tv_sec - myclock[0].tv_sec; \
		temp_n = myclock[1].tv_nsec - myclock[0].tv_nsec; \
		timedelay = BILLION * temp + temp_n; \
	} else { \
		temp = myclock[1].tv_sec - myclock[0].tv_sec - 1; \
		temp_n = BILLION + myclock[1].tv_nsec - myclock[0].tv_nsec; \
		timedelay = BILLION * temp + temp_n; \
	} \
	__sync_fetch_and_add(total_time, timedelay); \
	__sync_fetch_and_add(total_count, 1); \
} while(0)

//Global function time/count variables
unsigned long long total_time, total_count;

unsigned long long total_run_formation_time, total_run_formation_count;
unsigned long long total_run_formation_load_time, total_run_formation_load_count;
unsigned long long total_run_formation_sort_time, total_run_formation_sort_count;
unsigned long long total_run_formation_store_time, total_run_formation_store_count;
unsigned long long total_merge_time, total_merge_count;
unsigned long long total_merge_push_time, total_merge_push_count;
unsigned long long total_merge_pop_time, total_merge_pop_count;


#endif
