#include "shared.h"
#include "feed_data.h"

void *feedHeartbeat(void *args){
    ThreadParamsFeedHeartbeat *params = (ThreadParamsFeedHeartbeat *)args;
    int buffer = FEED_SELECT_BREAK;

    while(params->stop == 0){
        pthread_mutex_lock(&params->mutexTime);
        long currentTime = getTimeMilliseconds();
        long lastHeartbeat = params->lastTimeHeartbeat;
        pthread_mutex_unlock(&params->mutexTime);
        if(currentTime > lastHeartbeat + 2 * HEARTBEAT_TIME_SECS * 1000){
            printf("[!] - Manager não está a dar sinais de vida!\n");
            close(params->fd_manager);
            unlink(PIPE_NAME_MANAGER); // se pipe do manager ainda existir é apagado
            pthread_mutex_lock(&params->mutexStop);
            params->stop = 1;
            pthread_mutex_unlock(&params->mutexStop);
            if(write(params->fd_feed,&buffer,sizeof(buffer)) == -1){
                perror("Erro de escrita no pipe!\n");
                exit(EXIT_FAILURE);
            }
            pthread_exit(NULL);
        }

        if(write(params->fd_manager,&params->pacoteHeartbeat,sizeof(heartbeatEnvelope)) == -1){ // envia heartbeat para manager
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }

        sleep(HEARTBEAT_TIME_SECS);
    }
    pthread_exit(NULL);
}

void processaStdin(char input[INPUT_BUFFER_MAX],FeedData *dadosFeed){

    input[strcspn(input,"\n")] = '\0'; // retira \n e coloca \0

    char *comando = strtok(input," "); // retira a parte do comando da string de input

    if(comando == NULL){
        printf("Nenhum comando foi introduzido.\n");
        return;
    }

    if(strcmp(comando,"msg") == 0){
        char *topicoInput = strtok(NULL," ");
        char *duracaoString = strtok(NULL," ");
        char *mensagemInput = strtok(NULL,"");

        if(topicoInput == NULL || duracaoString == NULL || mensagemInput == NULL){
            printf("Uso: msg <topico> <duração> <mensagem>\n");
            return;
        }

        if(strlen(topicoInput) > TAM_TOPICO){
            printf("[!] - Topico tem no máximo %d caracteres. Tente novamente.\n",TAM_TOPICO);
            return;
        }

        if(strlen(mensagemInput) > TAM_MSG){
            printf("[!] - Mensagem tem no máximo %d caracteres. Tente novamente.\n",TAM_MSG);
            return;
        }

        char *endptr;
        errno = 0; // reset errno

        long duracaoLong = strtol(duracaoString,&endptr,10);

        if(errno == ERANGE){
            printf("[!] - Erro: Duração introduzida fora dos limites.\n");
            return;
        }
        if(endptr == duracaoString){
            printf("[!] - Erro: Foram introduzidos caracteres na duração da mensagem.\n");
            return;
        }
        if(*endptr != '\0'){
            printf("[!] - Erro: Foram introduzidos caracteres na duração da mensagem depois dos digitos: %s\n", endptr);
            return;
        }

        if(duracaoLong == 0){
            msgEnvelope pacoteMsg;
            memset(&pacoteMsg,0,sizeof(msgEnvelope));
            strcpy(pacoteMsg.info.username,dadosFeed->username);
            strcpy(pacoteMsg.info.topico,topicoInput);
            strcpy(pacoteMsg.info.mensagem,mensagemInput);
            pacoteMsg.tipo = INSTANT_MESSAGE;
            if(write(dadosFeed->pipe_manager_fd,&pacoteMsg,sizeof(pacoteMsg)) == -1){
                perror("Erro de escrita no pipe!\n");
                exit(EXIT_FAILURE);
            }
            return;
        }

        if(duracaoLong < 0){
            printf("[!] - O tempo de vida da mensagem não pode ser negativo: %ld\n",duracaoLong);
            return;
        }

        if(duracaoLong > INT_MAX){ // verifica se long cabe em int
            printf("[!] - Tempo de vida da mensagem demasiado grande.\n");
            return;
        }

        int duracao = (int)duracaoLong; // conversão de long para int (cast)
        msgpEnvelope pacoteMsgp;
        memset(&pacoteMsgp,0,sizeof(msgpEnvelope));
        strcpy(pacoteMsgp.info.username,dadosFeed->username);
        pacoteMsgp.info.tempoDeVida = duracao;
        strcpy(pacoteMsgp.info.topico, topicoInput);
        strcpy(pacoteMsgp.info.mensagem, mensagemInput);
        pacoteMsgp.tipo = PERSISTENT_MESSAGE;
        if(write(dadosFeed->pipe_manager_fd,&pacoteMsgp,sizeof(pacoteMsgp)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        return;

    }

    else if(strcmp(comando,"topics") == 0){
        topicsEnvelope pacoteTopics;
        memset(&pacoteTopics,0,sizeof(topicsEnvelope));
        strcpy(pacoteTopics.info.username,dadosFeed->username);
        pacoteTopics.tipo = COMMAND_TOPICS;
        if(write(dadosFeed->pipe_manager_fd,&pacoteTopics,sizeof(pacoteTopics)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        return;
    }

    else if(strcmp(comando,"subscribe") == 0){
        char *topicoInput = strtok(NULL," ");
        if(topicoInput == NULL){
            printf("Uso: subscribe <topico>\n");
            return;
        }

        if(strlen(topicoInput) > TAM_TOPICO){
            printf("[!] - Topico tem no máximo %d caracteres. Tente novamente.\n",TAM_TOPICO);
            return;
        }

        subscribeEnvelope pacoteSubscribe;
        memset(&pacoteSubscribe,0,sizeof(subscribeEnvelope));
        strcpy(pacoteSubscribe.info.username,dadosFeed->username);
        strcpy(pacoteSubscribe.info.topico,topicoInput);
        pacoteSubscribe.tipo = COMMAND_SUBSCRIBE;
        if(write(dadosFeed->pipe_manager_fd,&pacoteSubscribe,sizeof(pacoteSubscribe)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        return;
    }

    else if(strcmp(comando,"unsubscribe") == 0){
        char *topicoInput = strtok(NULL," ");
        if(topicoInput == NULL){
            printf("Uso: unsubscribe <topico>\n");
            return;
        }

        if(strlen(topicoInput) > TAM_TOPICO){
            printf("[!] - Topico tem no máximo %d caracteres. Tente novamente.\n",TAM_TOPICO);
            return;
        }

        unsubscribeEnvelope pacoteUnsubscribe;
        memset(&pacoteUnsubscribe,0,sizeof(unsubscribeEnvelope));
        strcpy(pacoteUnsubscribe.info.username,dadosFeed->username);
        strcpy(pacoteUnsubscribe.info.topico,topicoInput);
        pacoteUnsubscribe.tipo = COMMAND_UNSUBSCRIBE;
        if(write(dadosFeed->pipe_manager_fd,&pacoteUnsubscribe,sizeof(pacoteUnsubscribe)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        return;
    }

    else if(strcmp(comando,"commands") == 0){
        printf("'topics' >> Obter a listagem de todos os tópicos.\n");
        printf("'msg' >> Enviar mensagem para um determinado tópico. Uso: msg <topico> <duração> <mensagem>\n");
        printf("'subscribe' >> Subscrever um determinado tópico. Uso: subscribe <topico>\n");
        printf("'unsubscribe' >> Deixar de subscrever um determinado tópico. Uso: unsubscribe <topico>\n");
        printf("'exit' >> Sai da plataforma e encerra o processo feed.\n");
        printf("'commands' >> Obter a listagem de todos os comandos disponíveis.\n");
        return;
    }

    else if(strcmp(comando,"exit") == 0){
        EXITenvelope pacoteEXIT;
        memset(&pacoteEXIT,0,sizeof(EXITenvelope));
        strcpy(pacoteEXIT.info.username,dadosFeed->username);
        pacoteEXIT.tipo = COMMAND_EXIT;
        if(write(dadosFeed->pipe_manager_fd,&pacoteEXIT,sizeof(pacoteEXIT)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        pthread_mutex_lock(&dadosFeed->mutexStop);
        dadosFeed->threadParams.stop = 1;
        pthread_mutex_unlock(&dadosFeed->mutexStop);
        pthread_join(dadosFeed->threadHeartbeat,NULL);
        pthread_mutex_destroy(&dadosFeed->mutexStop);
        pthread_mutex_destroy(&dadosFeed->mutexTime);
        close(dadosFeed->pipe_feed_fd);
        unlink(dadosFeed->feedPipeName);
        printf("A encerrar...\n");
        exit(0);
    }

    else{
        printf("Comando desconhecido: %s\n",comando);
        return;
    }
}

void processaPipe(int tipo,FeedData *dadosFeed){

    //BUFFERS
    erroLogin erroLoginRecebido;
    respostaLogin respostaLoginRecebido;
    erroMsg erroMsgRecebido;
    respostaMsgTopicoCriado respostaMsgTopicoCriadoRecebido;
    respostaMsg respostaMsgRecebido;
    erroTopics erroTopicsRecebido;
    respostaTopics respostaTopicsRecebido;
    erroSubscribe erroSubscribeRecebido;
    respostaSubscribeCriado respostaSubscribeCriadoRecebido;
    respostaSubscribe respostaSubscribeRecebido;
    erroUnsubscribe erroUnsubscribeRecebido;
    respostaUnsubscribe respostaUnsubscribeRecebido;
    REMOVE REMOVErecebido;
    CLOSE CLOSErecebido;
    lock lockRecebido;
    unlock unlockRecebido;
    memset(&erroLoginRecebido,0,sizeof(erroLogin));
    memset(&respostaLoginRecebido,0,sizeof(respostaLogin));
    memset(&erroMsgRecebido,0,sizeof(erroMsg));
    memset(&respostaMsgTopicoCriadoRecebido,0,sizeof(respostaMsgTopicoCriado));
    memset(&respostaMsgRecebido,0,sizeof(respostaMsg));
    memset(&erroTopicsRecebido,0,sizeof(erroTopics));
    memset(&respostaTopicsRecebido,0,sizeof(respostaTopics));
    memset(&erroSubscribeRecebido,0,sizeof(erroSubscribe));
    memset(&respostaSubscribeCriadoRecebido,0,sizeof(respostaSubscribeCriado));
    memset(&respostaSubscribeRecebido,0,sizeof(respostaSubscribe));
    memset(&erroUnsubscribeRecebido,0,sizeof(erroUnsubscribe));
    memset(&respostaUnsubscribeRecebido,0,sizeof(respostaUnsubscribe));
    memset(&REMOVErecebido,0,sizeof(REMOVE));
    memset(&CLOSErecebido,0,sizeof(CLOSE));
    memset(&lockRecebido,0,sizeof(lock));
    memset(&unlockRecebido,0,sizeof(unlock));

    switch(tipo){
        case MAX_CLIENTS_EXCEEDED:
            read(dadosFeed->pipe_feed_fd,&erroLoginRecebido,sizeof(erroLogin));
            printf("%s\n",erroLoginRecebido.mensagemErroLogin);
            printf("A encerrar feed...\n");
            pthread_mutex_lock(&dadosFeed->mutexStop);
            dadosFeed->threadParams.stop = 1;
            pthread_mutex_unlock(&dadosFeed->mutexStop);
            pthread_join(dadosFeed->threadHeartbeat,NULL);
            close(dadosFeed->pipe_feed_fd);
            unlink(dadosFeed->feedPipeName);
            pthread_mutex_destroy(&dadosFeed->mutexStop);
            pthread_mutex_destroy(&dadosFeed->mutexTime);
            exit(0);

        case WELCOME:
            read(dadosFeed->pipe_feed_fd,&respostaLoginRecebido,sizeof(respostaLogin));
            printf("%s",respostaLoginRecebido.mensagemEntrada);
            break;

        case MESSAGE_ERROR:
            read(dadosFeed->pipe_feed_fd,&erroMsgRecebido,sizeof(erroMsg));
            printf("%s",erroMsgRecebido.mensagemErroMsg);
            break;

        case MSG_CREATED_TOPIC_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaMsgTopicoCriadoRecebido,sizeof(respostaMsgTopicoCriado));
            printf("Tópico %s criado.\n",respostaMsgTopicoCriadoRecebido.topico);
            printf("%s",respostaMsgTopicoCriadoRecebido.mensagem);
            break;

        case MESSAGE_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaMsgRecebido,sizeof(respostaMsg));
            printf("%s",respostaMsgRecebido.mensagem);
            break;

        case NO_TOPICS_AVAILABLE:
            read(dadosFeed->pipe_feed_fd,&erroTopicsRecebido,sizeof(erroTopics));
            printf("%s",erroTopicsRecebido.mensagemErroTopics);
            break;

        case TOPICS_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaTopicsRecebido,sizeof(respostaTopics));
            for(int i = 0; i < respostaTopicsRecebido.numTopicos; i++){
                printf("%s\n",respostaTopicsRecebido.arrayTopics[i]);
            }
            break;

        case SUBSCRIBE_ERROR:
            read(dadosFeed->pipe_feed_fd,&erroSubscribeRecebido,sizeof(erroSubscribe));
            printf("%s",erroSubscribeRecebido.mensagemErro);
            break;

        case SUBSCRIBE_CREATED_TOPIC_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaSubscribeCriadoRecebido,sizeof(respostaSubscribeCriado));
            printf("Tópico %s criado.\n",respostaSubscribeCriadoRecebido.topico);
            break;

        case SUBSCRIBE_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaSubscribeRecebido,sizeof(respostaSubscribe));
            printf("Tópico %s subscrito.\n",respostaSubscribeRecebido.topico);
            if(respostaSubscribeRecebido.numMensagensPersistentes > 0){
                printf("Mensagens persistentes armazenadas no tópico:\n");
                for(int i = 0; i < respostaSubscribeRecebido.numMensagensPersistentes; i++){
                    printf("%s\n", respostaSubscribeRecebido.arrayMensagensPersistentes[i]);
                }
            }
            break;

        case UNSUBSCRIBE_ERROR:
            read(dadosFeed->pipe_feed_fd,&erroUnsubscribeRecebido,sizeof(erroUnsubscribe));
            printf("%s",erroUnsubscribeRecebido.mensagemErro);
            break;

        case UNSUBSCRIBE_RESPONSE:
            read(dadosFeed->pipe_feed_fd,&respostaUnsubscribeRecebido,sizeof(respostaUnsubscribe));
            printf("Foi cancelada subscrição ao tópico %s.\n",respostaUnsubscribeRecebido.topico);
            break;

        case DISCONNECT:
            read(dadosFeed->pipe_feed_fd,&REMOVErecebido,sizeof(REMOVE));
            if(strcmp(dadosFeed->username,REMOVErecebido.username) != 0){
                printf("O utilizador %s foi removido da plataforma pelo administrador.\n",REMOVErecebido.username);
                break;
            }else{
                printf("[!] - Foi removido da plataforma pelo administrador.\n");
                pthread_mutex_lock(&dadosFeed->mutexStop);
                dadosFeed->threadParams.stop = 1;
                pthread_mutex_unlock(&dadosFeed->mutexStop);
                pthread_join(dadosFeed->threadHeartbeat,NULL);
                close(dadosFeed->pipe_feed_fd);
                unlink(dadosFeed->feedPipeName);
                pthread_mutex_destroy(&dadosFeed->mutexStop);
                pthread_mutex_destroy(&dadosFeed->mutexTime);
                exit(0);
            }

        case TOPIC_LOCK:
            read(dadosFeed->pipe_feed_fd,&lockRecebido,sizeof(lock));
            printf("[!] - Tópico %s foi bloqueado pelo administrador.\n",lockRecebido.topico);
            break;

        case TOPIC_UNLOCK:
            read(dadosFeed->pipe_feed_fd,&unlockRecebido,sizeof(unlock));
            printf("[!] - Tópico %s foi desbloqueado pelo administrador.\n",unlockRecebido.topico);
            break;

        case MANAGER_CLOSE:
            read(dadosFeed->pipe_feed_fd,&CLOSErecebido,sizeof(CLOSE));
            printf("%s",CLOSErecebido.mensagemSaida);
            pthread_mutex_lock(&dadosFeed->mutexStop);
            dadosFeed->threadParams.stop = 1;
            pthread_mutex_unlock(&dadosFeed->mutexStop);
            pthread_join(dadosFeed->threadHeartbeat,NULL);
            close(dadosFeed->pipe_feed_fd);
            unlink(dadosFeed->feedPipeName);
            pthread_mutex_destroy(&dadosFeed->mutexStop);
            pthread_mutex_destroy(&dadosFeed->mutexTime);
            exit(0);

        case MANAGER_HEARTBEAT:
            pthread_mutex_lock(&dadosFeed->mutexTime);
            dadosFeed->threadParams.lastTimeHeartbeat = getTimeMilliseconds(); // atualiza heartbeat
            pthread_mutex_unlock(&dadosFeed->mutexTime);
            break;

        default:
            break;
    }
}

int main(const int argc,const char *argv[]){

    FeedData dadosFeed;
    memset(&dadosFeed,0,sizeof(FeedData));

    struct sigaction sa;
    memset(&sa,0,sizeof(sa));
    sa.sa_handler = trata_sigint;

    if(sigaction(SIGINT,&sa,NULL) == -1){
        perror("Erro ao configurar sigaction.\n");
        exit(EXIT_FAILURE);
    }

    fd_set read_fds;
    int max_fd;
    int tipo;
    char inputBuffer[INPUT_BUFFER_MAX];

    dadosFeed.pipe_manager_fd = open(PIPE_NAME_MANAGER,O_RDWR);
    if(dadosFeed.pipe_manager_fd == -1){
        printf("[!] - Erro ao abrir named pipe: manager não está a correr.\n");
        exit(EXIT_FAILURE);
    }

    if(argc < 2){
        printf("[!] - Indique o seu username na linha de comandos.\n");
        exit(EXIT_FAILURE);
    }
    if(argc > 2){
        printf("[!] - Indique apenas o seu username na linha de comandos.\n");
        exit(EXIT_FAILURE);
    }

    if(strlen(argv[1]) > TAM_USER){
        printf("[!] - Username tem no máximo %d caracteres.\n",TAM_USER);
        exit(EXIT_FAILURE);
    }

    loginEnvelope pacoteLogin;
    memset(&pacoteLogin,0,sizeof(loginEnvelope));

    strcpy(pacoteLogin.info.username,argv[1]);
    strcpy(dadosFeed.username,pacoteLogin.info.username);

    snprintf(pacoteLogin.info.feedPipeName,sizeof(pacoteLogin.info.feedPipeName),"pipe_feed_%s",pacoteLogin.info.username);
    strcpy(dadosFeed.feedPipeName,pacoteLogin.info.feedPipeName);

    if(mkfifo(pacoteLogin.info.feedPipeName,0666) == -1){
        printf("[!] - Username %s já está a ser utilizado.\n", pacoteLogin.info.username);
        exit(EXIT_FAILURE);
    }

    dadosFeed.pipe_feed_fd = open(pacoteLogin.info.feedPipeName,O_RDWR);
    if(dadosFeed.pipe_feed_fd == -1){
        printf("[!] - Erro ao abrir o named pipe do feed %s.\n",pacoteLogin.info.username);
        exit(EXIT_FAILURE);
    }

    pacoteLogin.tipo = LOGIN;
    write(dadosFeed.pipe_manager_fd,&pacoteLogin,sizeof(pacoteLogin));

    // protege flag de paragem das threads
    pthread_mutex_init(&dadosFeed.mutexStop,NULL);
    // protege temporização do heartbeat
    pthread_mutex_init(&dadosFeed.mutexTime,NULL);

    dadosFeed.threadParams.stop = 0;
    dadosFeed.threadParams.fd_manager = dadosFeed.pipe_manager_fd;
    dadosFeed.threadParams.fd_feed = dadosFeed.pipe_feed_fd;
    strcpy(dadosFeed.threadParams.nomePipeFeed,pacoteLogin.info.feedPipeName);
    strcpy(dadosFeed.threadParams.pacoteHeartbeat.info.username,pacoteLogin.info.username);
    dadosFeed.threadParams.lastTimeHeartbeat = getTimeMilliseconds();
    dadosFeed.threadParams.pacoteHeartbeat.tipo = FEED_HEARTBEAT;
    dadosFeed.threadParams.mutexStop = dadosFeed.mutexStop;
    dadosFeed.threadParams.mutexTime = dadosFeed.mutexTime;

    if(pthread_create(&dadosFeed.threadHeartbeat,NULL,feedHeartbeat, (void *) &dadosFeed.threadParams) != 0){
        perror("[!] - Erro ao criar a thread.\n");
        exit(EXIT_FAILURE);
    }

    while(1){

        pthread_mutex_lock(&dadosFeed.mutexStop);
        if(dadosFeed.threadParams.stop == 1){
            pthread_mutex_unlock(&dadosFeed.mutexStop);
            break;
        }
        pthread_mutex_unlock(&dadosFeed.mutexStop);

        // SELECT LOOP

        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds);
        FD_SET(dadosFeed.pipe_feed_fd, &read_fds);
        max_fd = (STDIN_FILENO > dadosFeed.pipe_feed_fd) ? STDIN_FILENO : dadosFeed.pipe_feed_fd;

        int ret = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if(ret == -1){
            perror("[!] - Erro no select.\n");
            break;
        }

        // Verifica se há dados disponíveis para leitura no named pipe
        if(FD_ISSET(dadosFeed.pipe_feed_fd, &read_fds)){
            ssize_t bytesLidos = read(dadosFeed.pipe_feed_fd,&tipo,sizeof(int));
            if(bytesLidos > 0){
                if(bytesLidos == FEED_SELECT_BREAK){
                    break;
                }else{
                    processaPipe(tipo,&dadosFeed);
                }
            }else if(bytesLidos == -1){
                perror("[!] - Erro ao ler do pipe do feed.\n");
                break;
            }
        }

        // Verifica se há dados disponíveis para leitura no stdin
        if(FD_ISSET(STDIN_FILENO, &read_fds)){
            ssize_t bytesLidos = read(STDIN_FILENO,inputBuffer,INPUT_BUFFER_MAX - 1);
            if(bytesLidos > 0){
                processaStdin(inputBuffer,&dadosFeed);
            }else if(bytesLidos == -1){
                perror("[!] - Erro ao ler do stdin.\n");
                break;
            }
        }
    }
    EXITenvelope pacoteEXIT;
    memset(&pacoteEXIT,0,sizeof(EXITenvelope));
    strcpy(pacoteEXIT.info.username,dadosFeed.username);
    pacoteEXIT.tipo = COMMAND_EXIT;
    if(write(dadosFeed.pipe_manager_fd,&pacoteEXIT,sizeof(pacoteEXIT)) == -1){
        perror("Erro de escrita no pipe!\n");
        exit(EXIT_FAILURE);
    }
    pthread_mutex_lock(&dadosFeed.mutexStop);
    dadosFeed.threadParams.stop = 1;
    pthread_mutex_unlock(&dadosFeed.mutexStop);
    pthread_join(dadosFeed.threadHeartbeat,NULL);
    close(dadosFeed.pipe_feed_fd);
    unlink(pacoteLogin.info.feedPipeName);
    pthread_mutex_destroy(&dadosFeed.mutexStop);
    pthread_mutex_destroy(&dadosFeed.mutexTime);
    printf("A encerrar feed...\n");
    exit(0);
}