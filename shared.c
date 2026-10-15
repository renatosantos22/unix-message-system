#include "shared.h"

long long getTimeMilliseconds(){
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME,&ts);

    return (ts.tv_sec * 1000LL) + (ts.tv_nsec / 1000000);
}

void trata_sigint(int signo){
    (void)signo; // para não haver warning
}