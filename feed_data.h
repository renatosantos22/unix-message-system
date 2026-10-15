#ifndef TRABALHO_FEED_DATA_H
#define TRABALHO_FEED_DATA_H

#include "shared.h"

typedef struct{
    int fd_manager;
    int fd_feed;
    char nomePipeFeed[32];
    int stop;
    long long lastTimeHeartbeat;
    heartbeatEnvelope pacoteHeartbeat;
    pthread_mutex_t mutexStop;
    pthread_mutex_t mutexTime;
} ThreadParamsFeedHeartbeat;

typedef struct{

    int pipe_feed_fd;
    int pipe_manager_fd;
    char username[TAM_USER];
    char feedPipeName[TAM_FEED_PIPE_NAME];
    pthread_mutex_t mutexStop;
    pthread_mutex_t mutexTime;
    ThreadParamsFeedHeartbeat threadParams;
    pthread_t threadHeartbeat;

} FeedData;

#endif