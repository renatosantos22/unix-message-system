#ifndef TRABALHO_MANAGER_DATA_H
#define TRABALHO_MANAGER_DATA_H

#include "shared.h"

typedef struct{
    char nomeAutor[TAM_USER];
    int clientesSubscritos;
    int locked;
    char nomeTopico[TAM_TOPICO];
    char mensagensPersistentes[MAX_MSGP][TAM_MSG];
    int temposDeVida[MAX_MSGP];
    int numMensagensPersistentes;
} armazenamentoTopicos;

typedef struct{
    char nomeCliente[TAM_USER];
    int pipe_fd_feed;
    int numTopicosSubscritos;
    char topicosSubscritos[MAX_TOPICOS][TAM_TOPICO];
    long long lastTimeHeartbeat;
} armazenamentoClientes;

typedef struct{
    armazenamentoTopicos *arrayTopicos;
    int *contadorTopicos;
    int stop;
    pthread_mutex_t mutexTopicos;
} ThreadParamsDecrementa;

typedef struct{
    armazenamentoClientes *arrayClientes;
    int *contadorClientes;
    armazenamentoTopicos *arrayTopicos;
    int *contadorTopicos;
    int stop;
    int *stopThreadDecrementa;
    int heartbeat;
    int fd_manager;
    const char *ficheiroMensagens;
    CLOSEenvelope pacoteCLOSE;
    pthread_mutex_t mutexTopicos;
    pthread_mutex_t mutexClientes;
    pthread_mutex_t mutexStop;
    pthread_mutex_t mutexTime;
} ThreadParamsManagerHeartbeat;

typedef struct{

    const char *ficheiroMensagens;
    armazenamentoTopicos arrayTopicos[MAX_TOPICOS];
    int contadorTopicos;
    armazenamentoClientes arrayClientes[MAX_CLIENTES];
    int contadorClientes;
    int pipe_manager_fd;
    pthread_mutex_t mutexTopicos;
    pthread_mutex_t mutexClientes;
    pthread_mutex_t mutexStop;
    pthread_mutex_t mutexTime;
    ThreadParamsDecrementa threadParamsTempos;
    pthread_t threadTemposDeVida;
    ThreadParamsManagerHeartbeat threadParamsHeartbeat;
    pthread_t threadHeartbeat;

} ManagerData;

#endif