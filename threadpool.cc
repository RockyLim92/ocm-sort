#include <pthread.h>
#include <vector>

class ThreadInfo{
private:
    pthread_cond_t condition_;
    pthread_mutex_t mutext_;

public:
    ThreadInfo(pthread_cond_t _cond, pthread_mutex_t _mutex);
    int TryLock();
    int Job();
    int UnLock();
};

ThreadInfo::ThreadInfo(pthread_cond_t _cond, pthread_mutex_t _mutex){
    

}
