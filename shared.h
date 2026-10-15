#ifndef TRABALHO_SHARED_H
#define TRABALHO_SHARED_H

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <pthread.h>

#define PIPE_NAME_MANAGER "manager_pipe"
#define TAM_MSG 300
#define TAM_USER 15
#define TAM_TOPICO 20
#define MAX_CLIENTES 10
#define INPUT_BUFFER_MAX 512
#define MAX_TOPICOS 20
#define MAX_MSGP 5
#define TAM_MSG_GERAL 128
#define HEARTBEAT_TIME_SECS 1
#define TAM_FEED_PIPE_NAME 32

enum PACKET_TYPE{
    MAX_CLIENTS_EXCEEDED = 1,
    WELCOME,
    MESSAGE_ERROR,
    MSG_CREATED_TOPIC_RESPONSE,
    MESSAGE_RESPONSE,
    NO_TOPICS_AVAILABLE,
    TOPICS_RESPONSE,
    SUBSCRIBE_ERROR,
    SUBSCRIBE_CREATED_TOPIC_RESPONSE,
    SUBSCRIBE_RESPONSE,
    UNSUBSCRIBE_ERROR,
    UNSUBSCRIBE_RESPONSE,
    DISCONNECT,
    TOPIC_LOCK,
    TOPIC_UNLOCK,
    MANAGER_CLOSE,
    MANAGER_HEARTBEAT,
    LOGIN=100,
    INSTANT_MESSAGE,
    PERSISTENT_MESSAGE,
    COMMAND_TOPICS,
    COMMAND_SUBSCRIBE,
    COMMAND_UNSUBSCRIBE,
    COMMAND_EXIT,
    FEED_HEARTBEAT,
    FEED_SELECT_BREAK,
};

//STRUCTS QUE O FEED PODE ENVIAR

typedef struct{
    char username[TAM_USER];
    char feedPipeName[TAM_FEED_PIPE_NAME];
} login;

typedef struct{
    int tipo;
    login info;
} loginEnvelope;

typedef struct{
    char mensagem[TAM_MSG];
    char topico[TAM_TOPICO];
    char username[TAM_USER];
} msg;

typedef struct{
    int tipo;
    msg info;
} msgEnvelope;

typedef struct{
    int tempoDeVida;
    char mensagem[TAM_MSG];
    char topico[TAM_TOPICO];
    char username[TAM_USER];
} msgp;

typedef struct{
    int tipo;
    msgp info;
} msgpEnvelope;

typedef struct{
    char username[TAM_USER];
} topics;

typedef struct{
    int tipo;
    topics info;
} topicsEnvelope;

typedef struct{
    char username[TAM_USER];
    char topico[TAM_TOPICO];
} subscribe;

typedef struct{
    int tipo;
    subscribe info;
} subscribeEnvelope;

typedef struct{
    char username[TAM_USER];
    char topico[TAM_TOPICO];
} unsubscribe;

typedef struct{
    int tipo;
    unsubscribe info;
} unsubscribeEnvelope;

typedef struct{
    char username[TAM_USER];
} EXIT;

typedef struct{
    int tipo;
    EXIT info;
} EXITenvelope;

typedef struct{
    char username[TAM_USER];
} heartbeat;

typedef struct{
    int tipo;
    heartbeat info;
} heartbeatEnvelope;

// STRUCTS QUE O MANAGER PODE ENVIAR

typedef struct{
    char username[TAM_USER];
} REMOVE;

typedef struct{
    int tipo;
    REMOVE info;
} REMOVEenvelope;

typedef struct{
    char mensagemSaida[TAM_MSG_GERAL];
} CLOSE;

typedef struct{
    int tipo;
    CLOSE info;
} CLOSEenvelope;

typedef struct{
    char topico[TAM_TOPICO];
} lock;

typedef struct{
    int tipo;
    lock info;
} lockEnvelope;

typedef struct{
    char topico[TAM_TOPICO];
} unlock;

typedef struct{
    int tipo;
    unlock info;
} unlockEnvelope;

typedef struct{
    char mensagemEntrada[TAM_MSG_GERAL];
} respostaLogin;

typedef struct{
    int tipo;
    respostaLogin info;
} respostaLoginEnvelope;

typedef struct{
    char mensagemErroLogin[TAM_MSG_GERAL];
} erroLogin;

typedef struct{
    int tipo;
    erroLogin info;
} erroLoginEnvelope;

typedef struct{
    char arrayTopics[MAX_TOPICOS][TAM_MSG_GERAL];
    int numTopicos;
} respostaTopics;

typedef struct{
    int tipo;
    respostaTopics info;
} respostaTopicsEnvelope;

typedef struct{
    char mensagemErroTopics[TAM_MSG_GERAL];
} erroTopics;

typedef struct{
    int tipo;
    erroTopics info;
} erroTopicsEnvelope;

typedef struct{
    char mensagem[TAM_USER+TAM_TOPICO+TAM_MSG+32];
} respostaMsg;

typedef struct{
    int tipo;
    respostaMsg info;
} respostaMsgEnvelope;

typedef struct{
    char topico[TAM_TOPICO];
    char mensagem[TAM_USER+TAM_TOPICO+TAM_MSG+32];
} respostaMsgTopicoCriado;

typedef struct{
    int tipo;
    respostaMsgTopicoCriado info;
} respostaMsgTopicoCriadoEnvelope;

typedef struct{
    char mensagemErroMsg[TAM_MSG_GERAL];
} erroMsg;

typedef struct{
    int tipo;
    erroMsg info;
} erroMsgEnvelope;

typedef struct{
    char topico[TAM_TOPICO];
    char arrayMensagensPersistentes[MAX_MSGP][TAM_MSG];
    int numMensagensPersistentes;
} respostaSubscribe;

typedef struct{
    int tipo;
    respostaSubscribe info;
} respostaSubscribeEnvelope;

typedef struct{
    char topico[TAM_TOPICO];
} respostaSubscribeCriado;

typedef struct{
    int tipo;
    respostaSubscribeCriado info;
} respostaSubscribeCriadoEnvelope;

typedef struct{
    char mensagemErro[TAM_MSG_GERAL];
} erroSubscribe;

typedef struct{
    int tipo;
    erroSubscribe info;
} erroSubscribeEnvelope;

typedef struct{
    char topico[TAM_TOPICO];
} respostaUnsubscribe;

typedef struct{
    int tipo;
    respostaUnsubscribe info;
} respostaUnsubscribeEnvelope;

typedef struct{
    char mensagemErro[TAM_MSG_GERAL];
} erroUnsubscribe;

typedef struct{
    int tipo;
    erroUnsubscribe info;
} erroUnsubscribeEnvelope;

long long getTimeMilliseconds();
void trata_sigint(int signo);

#endif