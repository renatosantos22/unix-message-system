#include "shared.h"
#include "manager_data.h"
#include "manager_aux.h"

void *decrementaTemposDeVida(void *args){
    ThreadParamsDecrementa *params = (ThreadParamsDecrementa *)args;

    while(params->stop == 0){
        pthread_mutex_lock(&params->mutexTopicos);
        for(int i = 0; i < *params->contadorTopicos; i++){
            if(strlen(params->arrayTopicos[i].nomeTopico) > 0){ // Verifica se o tópico existe
                for(int j = 0; j < params->arrayTopicos[i].numMensagensPersistentes; j++){
                    if(params->arrayTopicos[i].temposDeVida[j] > 0){
                        params->arrayTopicos[i].temposDeVida[j]--;

                        // Remove a mensagem se o tempo de vida chegar a 0
                        if(params->arrayTopicos[i].temposDeVida[j] == 0){
                            printf("Mensagem expirou no tópico %s: %s\n",params->arrayTopicos[i].nomeTopico,params->arrayTopicos[i].mensagensPersistentes[j]);

                            // Reorganiza as mensagens
                            for(int k = j; k < params->arrayTopicos[i].numMensagensPersistentes; k++){
                                strcpy(params->arrayTopicos[i].mensagensPersistentes[k], params->arrayTopicos[i].mensagensPersistentes[k + 1]);
                                params->arrayTopicos[i].temposDeVida[k] = params->arrayTopicos[i].temposDeVida[k + 1];
                            }

                            // Limpa a última mensagem após a reorganização
                            memset(params->arrayTopicos[i].mensagensPersistentes[params->arrayTopicos[i].numMensagensPersistentes - 1],0,TAM_MSG);
                            params->arrayTopicos[i].temposDeVida[params->arrayTopicos[i].numMensagensPersistentes] = 0;

                            params->arrayTopicos[i].numMensagensPersistentes--;
                            j--; // Reavalia a posição atual após reorganizar
                        }
                    }
                }

                // Verifica se o tópico deve ser eliminado
                if(params->arrayTopicos[i].clientesSubscritos == 0 && params->arrayTopicos[i].numMensagensPersistentes == 0){
                    printf("Tópico %s não tem clientes subscritos nem mensagens persistentes logo foi removido.\n",params->arrayTopicos[i].nomeTopico);
                    eliminaTopico(params->arrayTopicos,params->contadorTopicos,params->arrayTopicos[i].nomeTopico);
                    i--; // Reavalia a posição atual após reorganizar os tópicos
                }
            }
        }

        pthread_mutex_unlock(&params->mutexTopicos);
        sleep(1);
    }
    pthread_exit(NULL);
}

void *managerHeartbeat(void *args){
    ThreadParamsManagerHeartbeat *params = (ThreadParamsManagerHeartbeat *)args;

    while(params->stop == 0){
        pthread_mutex_lock(&params->mutexClientes);
        for(int i = 0; i < *params->contadorClientes; i++){
            pthread_mutex_lock(&params->mutexTime);
            long currentTime = getTimeMilliseconds();
            long lastHeartbeat = params->arrayClientes[i].lastTimeHeartbeat;
            pthread_mutex_unlock(&params->mutexTime);
            if(currentTime > lastHeartbeat + 2 * HEARTBEAT_TIME_SECS * 1000){
                // cliente vai ser descartado
                printf("[!] - Cliente %s não está a dar sinais de vida e vai ser eliminado.\n",params->arrayClientes[i].nomeCliente);
                pthread_mutex_lock(&params->mutexTopicos);
                eliminaCliente(params->arrayClientes,params->arrayTopicos,params->contadorClientes,params->contadorTopicos,params->arrayClientes[i].nomeCliente);
                pthread_mutex_unlock(&params->mutexTopicos);
                pthread_mutex_unlock(&params->mutexClientes);
            }else{
                int fdTemp = devolveFdFeed(params->arrayClientes,*params->contadorClientes,params->arrayClientes[i].nomeCliente);
                pthread_mutex_unlock(&params->mutexClientes);
                pthread_mutex_lock(&params->mutexTime);
                int bufTemp = params->heartbeat;
                pthread_mutex_unlock(&params->mutexTime);
                int tamTemp = sizeof(int);
                if(write(fdTemp,&bufTemp,tamTemp) == -1){ // envia heartbeat para o feed
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
            }
            pthread_mutex_lock(&params->mutexClientes);
        }
        pthread_mutex_unlock(&params->mutexClientes);
        sleep(HEARTBEAT_TIME_SECS);
    }
    pthread_exit(NULL);
}


void processaStdin(char input[INPUT_BUFFER_MAX],ManagerData *dadosManager){

    input[strcspn(input,"\n")] = '\0'; // retira \n e coloca \0

    char *comando = strtok(input," "); // retira a parte do comando da string de input

    if(comando == NULL){
        printf("Nenhum comando foi introduzido.\n");
        return;
    }

    if(strcmp(comando,"users") == 0){
        pthread_mutex_lock(&dadosManager->mutexClientes);
        listaUtilizadoresNaPlataforma(dadosManager->arrayClientes,dadosManager->contadorClientes);
        pthread_mutex_unlock(&dadosManager->mutexClientes);
        return;
    }

    else if(strcmp(comando,"remove") == 0){
        char *usernameInput = strtok(NULL," ");
        if(usernameInput == NULL){
            printf("Uso: remove <username>\n");
            return;
        }
        pthread_mutex_lock(&dadosManager->mutexClientes);
        if(verificaUsernameRepetido(dadosManager->arrayClientes,dadosManager->contadorClientes,usernameInput) == 0){
            printf("Username: %s não se encontra ligado á plataforma.\n",usernameInput);
            pthread_mutex_unlock(&dadosManager->mutexClientes);
            return;
        }
        pthread_mutex_unlock(&dadosManager->mutexClientes);
        REMOVEenvelope pacoteREMOVE;
        memset(&pacoteREMOVE,0,sizeof(REMOVEenvelope));
        strcpy(pacoteREMOVE.info.username,usernameInput);
        pacoteREMOVE.tipo = DISCONNECT;
        pthread_mutex_lock(&dadosManager->mutexClientes);
        for(int i = 0; i < dadosManager->contadorClientes; i++){
            int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
            pthread_mutex_unlock(&dadosManager->mutexClientes);
            if(write(fdTemp,&pacoteREMOVE,sizeof(pacoteREMOVE)) == -1){ // avisa clientes que cliente foi removido
                perror("Erro de escrita no pipe!\n");
                exit(EXIT_FAILURE);
            }
            pthread_mutex_lock(&dadosManager->mutexClientes);
        }
        close(devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,usernameInput)); // fecha pipe do cliente introduzido
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        eliminaCliente(dadosManager->arrayClientes,dadosManager->arrayTopicos,&dadosManager->contadorClientes,&dadosManager->contadorTopicos,usernameInput); // elimina cliente
        pthread_mutex_unlock(&dadosManager->mutexTopicos);
        pthread_mutex_unlock(&dadosManager->mutexClientes);
        printf("Utilizador %s removido da plataforma.\n",usernameInput);
        return;
    }

    else if(strcmp(comando,"topics") == 0){
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        listaTopicosNaPlataforma(dadosManager->arrayTopicos,dadosManager->contadorTopicos);
        pthread_mutex_unlock(&dadosManager->mutexTopicos);
        return;
    }

    else if(strcmp(comando,"show") == 0){
        char *topicoInput = strtok(NULL," ");
        if(topicoInput == NULL){
            printf("Uso: show <topico>\n");
            return;
        }
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput) == 0){
            pthread_mutex_unlock(&dadosManager->mutexTopicos);
            printf("Topico %s não se encontra registado na plataforma.\n",topicoInput);
            return;
        }else{
            listaMensagensPersistentes(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput);
            pthread_mutex_unlock(&dadosManager->mutexTopicos);
            return;
        }
    }

    else if(strcmp(comando,"lock") == 0){
        char *topicoInput = strtok(NULL," ");
        if(topicoInput == NULL){
            printf("Uso: lock <topico>\n");
            return;
        }
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput) == 0){
            pthread_mutex_unlock(&dadosManager->mutexTopicos);
            printf("Tópico %s não se encontra registado na plataforma.\n",topicoInput);
            return;
        }else{
            if(bloqueiaTopico(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput) == 1){
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                printf("Tópico %s bloqueado.\n",topicoInput);
                lockEnvelope pacoteLock;
                memset(&pacoteLock,0,sizeof(lockEnvelope));
                strcpy(pacoteLock.info.topico,topicoInput);
                pacoteLock.tipo = TOPIC_LOCK;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                for(int i = 0; i < dadosManager->contadorClientes; i++){
                    for(int j = 0; j < dadosManager->arrayClientes[i].numTopicosSubscritos; j++){
                        if(strcmp(dadosManager->arrayClientes[i].topicosSubscritos[j],topicoInput) == 0){
                            int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
                            pthread_mutex_unlock(&dadosManager->mutexClientes);
                            if(write(fdTemp,&pacoteLock,sizeof(pacoteLock)) == -1){
                                perror("Erro de escrita no pipe!\n");
                                exit(EXIT_FAILURE);
                            }
                            pthread_mutex_lock(&dadosManager->mutexClientes);
                        }
                    }
                }
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                return;
            }else{
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                printf("Tópico %s já se encontra bloqueado.\n",topicoInput);
                return;
            }
        }
    }

    else if(strcmp(comando,"unlock") == 0){
        char *topicoInput = strtok(NULL," ");
        if(topicoInput == NULL){
            printf("Uso: unlock <topico>\n");
            return;
        }
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput) == 0){
            pthread_mutex_unlock(&dadosManager->mutexTopicos);
            printf("Topico %s não se encontra registado na plataforma.\n",topicoInput);
            return;
        }else{
            if(desbloqueiaTopico(dadosManager->arrayTopicos,dadosManager->contadorTopicos,topicoInput) == 1){
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                printf("Tópico %s desbloqueado.\n",topicoInput);
                unlockEnvelope pacoteUnlock;
                memset(&pacoteUnlock,0,sizeof(unlockEnvelope));
                strcpy(pacoteUnlock.info.topico,topicoInput);
                pacoteUnlock.tipo = TOPIC_UNLOCK;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                for(int i = 0; i < dadosManager->contadorClientes; i++){
                    for(int j = 0; j < dadosManager->arrayClientes[i].numTopicosSubscritos; j++){
                        if(strcmp(dadosManager->arrayClientes[i].topicosSubscritos[j],topicoInput) == 0){
                            int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
                            pthread_mutex_unlock(&dadosManager->mutexClientes);
                            if(write(fdTemp,&pacoteUnlock,sizeof(pacoteUnlock)) == -1){
                                perror("Erro de escrita no pipe!\n");
                                exit(EXIT_FAILURE);
                            }
                            pthread_mutex_lock(&dadosManager->mutexClientes);
                        }
                    }
                }
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                return;
            }else{
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                printf("Tópico %s já se encontra desbloqueado.\n",topicoInput);
                return;
            }
        }
    }

    else if(strcmp(comando,"commands") == 0){
        printf("'users' >> Listar utilizadores atualmente a usar a plataforma.\n");
        printf("'remove' >> Eliminar um utilizador. Uso: remove <username>\n");
        printf("'topics' >> Listar os tópicos existentes na plataforma.\n");
        printf("'show' >> Listar as mensagens existentes num determinado tópico. Uso: show <topico>\n");
        printf("'lock' >> Bloquear um tópico. Uso: lock <topico>\n");
        printf("'unlock' >> Desbloquear um tópico. Uso: unlock <topico>\n");
        printf("'close' >> Encerrar a plataforma.\n");
        printf("'commands' >> Obter a listagem de todos os comandos disponíveis.\n");
        return;
    }

    else if(strcmp(comando,"close") == 0){
        CLOSEenvelope pacoteCLOSE;
        memset(&pacoteCLOSE,0,sizeof(CLOSEenvelope));
        pacoteCLOSE.tipo = MANAGER_CLOSE;
        strcpy(pacoteCLOSE.info.mensagemSaida,"Manager foi encerrado.\n");
        pthread_mutex_lock(&dadosManager->mutexClientes);
        for(int i = 0; i < dadosManager->contadorClientes; i++){
            int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
            pthread_mutex_unlock(&dadosManager->mutexClientes);
            if(write(fdTemp,&pacoteCLOSE,sizeof(pacoteCLOSE)) == -1){
                perror("Erro de escrita no pipe!\n");
                exit(EXIT_FAILURE);
            }
            close(dadosManager->arrayClientes[i].pipe_fd_feed);
            pthread_mutex_lock(&dadosManager->mutexClientes);
        }
        pthread_mutex_unlock(&dadosManager->mutexClientes);
        pthread_mutex_lock(&dadosManager->mutexStop);
        dadosManager->threadParamsTempos.stop = 1; // manda thread terminar
        dadosManager->threadParamsHeartbeat.stop = 1;
        pthread_mutex_unlock(&dadosManager->mutexStop);
        pthread_join(dadosManager->threadTemposDeVida,NULL);
        pthread_join(dadosManager->threadHeartbeat,NULL);
        pthread_mutex_lock(&dadosManager->mutexTopicos);
        salvarMensagensPersistentes(dadosManager->arrayTopicos,dadosManager->contadorTopicos,dadosManager->ficheiroMensagens);
        pthread_mutex_unlock(&dadosManager->mutexTopicos);
        pthread_mutex_destroy(&dadosManager->mutexTopicos);
        pthread_mutex_destroy(&dadosManager->mutexClientes);
        pthread_mutex_destroy(&dadosManager->mutexTime);
        pthread_mutex_destroy(&dadosManager->mutexStop);
        close(dadosManager->pipe_manager_fd);
        unlink(PIPE_NAME_MANAGER);
        printf("Manager a encerrar...\n");
        exit(0);
    }

    else{
        printf("Comando desconhecido: %s\n",comando);
        return;
    }
}

void processaPipe(int tipo,ManagerData *dadosManager){

    //BUFFERS
    login loginRecebido;
    msg msgRecebido;
    msgp msgpRecebido;
    topics topicsRecebido;
    subscribe subscribeRecebido;
    unsubscribe unsubscribeRecebido;
    EXIT EXITRecebido;
    heartbeat heartbeatRecebido;
    memset(&loginRecebido,0,sizeof(login));
    memset(&msgRecebido,0,sizeof(msg));
    memset(&msgpRecebido,0,sizeof(msgp));
    memset(&topicsRecebido,0,sizeof(topics));
    memset(&subscribeRecebido,0,sizeof(subscribe));
    memset(&unsubscribeRecebido,0,sizeof(unsubscribe));
    memset(&EXITRecebido,0,sizeof(EXIT));
    memset(&heartbeatRecebido,0,sizeof(heartbeat));

    switch(tipo){
        case LOGIN:
            read(dadosManager->pipe_manager_fd,&loginRecebido,sizeof(login));

            int pipe_feed_fd_temp = open(loginRecebido.feedPipeName,O_RDWR); // pipe do feed aberto pela primeira vez
            if(pipe_feed_fd_temp == -1){
                printf("Erro ao abrir o named pipe do feed %s.\n",loginRecebido.username);
                exit(EXIT_FAILURE);
            }

            pthread_mutex_lock(&dadosManager->mutexClientes);
            if(adicionaCliente(dadosManager->arrayClientes,&dadosManager->contadorClientes,loginRecebido.username,pipe_feed_fd_temp) == 0){
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                erroLoginEnvelope pacoteErroLogin;
                memset(&pacoteErroLogin,0,sizeof(erroLoginEnvelope));
                strcpy(pacoteErroLogin.info.mensagemErroLogin,"[!] - Não existe espaço na plataforma para mais um utilizador.\n");
                pacoteErroLogin.tipo = MAX_CLIENTS_EXCEEDED;
                if(write(pipe_feed_fd_temp,&pacoteErroLogin,sizeof(pacoteErroLogin)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                close(pipe_feed_fd_temp);
                break;
            }else{ // se cliente for adicionado
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                respostaLoginEnvelope pacoteRespostaLogin;
                memset(&pacoteRespostaLogin,0,sizeof(respostaLoginEnvelope));
                snprintf(pacoteRespostaLogin.info.mensagemEntrada,sizeof(pacoteRespostaLogin.info.mensagemEntrada),"Seja bem vindo á plataforma %s. Escreva 'commands' para uma lista de comandos disponíveis.\n",dadosManager->arrayClientes[dadosManager->contadorClientes - 1].nomeCliente);
                pacoteRespostaLogin.tipo = WELCOME;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,loginRecebido.username);
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                if(write(fdTemp,&pacoteRespostaLogin,sizeof(pacoteRespostaLogin)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                printf("O utlizador %s entrou na plataforma.\n",loginRecebido.username);
                // aqui não fecho o pipe do cliente que acabou de entrar
                break;
            }

        case INSTANT_MESSAGE:
            read(dadosManager->pipe_manager_fd,&msgRecebido,sizeof(msg));

            pthread_mutex_lock(&dadosManager->mutexTopicos);
            if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgRecebido.topico) == 0){
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                // topico nao existe e mensagem tem tempo de vida zero logo manda erro (comando seria inutil)
                erroMsgEnvelope pacoteErroMsg;
                memset(&pacoteErroMsg,0,sizeof(erroMsgEnvelope));
                snprintf(pacoteErroMsg.info.mensagemErroMsg,sizeof(pacoteErroMsg.info.mensagemErroMsg),"[!] - Tópico %s não foi criado. Não é possivel mandar uma mensagem não persistente para um topico que ainda não existe.\n",msgRecebido.topico);
                pacoteErroMsg.tipo = MESSAGE_ERROR;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgRecebido.username);
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                if(write(fdTemp,&pacoteErroMsg,sizeof(pacoteErroMsg)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                break;
            }else{
                // topico existe
                if(verificaTopicoBloqueado(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgRecebido.topico) == 1){
                    // se topico estiver bloqueado manda erro para o feed
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    erroMsgEnvelope pacoteErroMsg;
                    memset(&pacoteErroMsg,0,sizeof(erroMsgEnvelope));
                    snprintf(pacoteErroMsg.info.mensagemErroMsg,sizeof(pacoteErroMsg.info.mensagemErroMsg),"[!] - O tópico: %s está bloqueado. Mensagem não enviada.\n",msgRecebido.topico);
                    pacoteErroMsg.tipo = MESSAGE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroMsg,sizeof(pacoteErroMsg)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    // manda mensagem instantanea para todos os feeds subscritos
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    respostaMsgEnvelope pacoteRespostaMsg;
                    memset(&pacoteRespostaMsg,0,sizeof(respostaMsgEnvelope));
                    snprintf(pacoteRespostaMsg.info.mensagem, sizeof(pacoteRespostaMsg.info.mensagem),"[%s] - %s: %s\n",msgRecebido.username,msgRecebido.topico,msgRecebido.mensagem);
                    pacoteRespostaMsg.tipo = MESSAGE_RESPONSE;
                    // manda mensagem imediatamente para os clientes subscritos
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    for(int i = 0; i < dadosManager->contadorClientes; i++){
                        for(int j = 0; j < dadosManager->arrayClientes[i].numTopicosSubscritos; j++){
                            if(strcmp(dadosManager->arrayClientes[i].topicosSubscritos[j], msgRecebido.topico) == 0){
                                int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
                                pthread_mutex_unlock(&dadosManager->mutexClientes);
                                if(write(fdTemp,&pacoteRespostaMsg,sizeof(pacoteRespostaMsg)) == -1){
                                    perror("Erro de escrita no pipe!\n");
                                    exit(EXIT_FAILURE);
                                }
                                pthread_mutex_lock(&dadosManager->mutexClientes);
                            }
                        }
                    }
                    // se cliente que mandou mensagem não estiver subscrito ao topico é mandada para ele a sua propria mensagem
                    if(verificaClienteSubscrito(dadosManager->arrayClientes,dadosManager->contadorClientes,msgRecebido.topico,msgRecebido.username) == 0){
                        int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgRecebido.username);
                        pthread_mutex_unlock(&dadosManager->mutexClientes);
                        if(write(fdTemp,&pacoteRespostaMsg,sizeof(pacoteRespostaMsg)) == -1){
                            perror("Erro de escrita no pipe!\n");
                            exit(EXIT_FAILURE);
                        }
                        pthread_mutex_lock(&dadosManager->mutexClientes);
                    }
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    break;
                }
            }

        case PERSISTENT_MESSAGE:
            read(dadosManager->pipe_manager_fd,&msgpRecebido,sizeof(msgp));

            pthread_mutex_lock(&dadosManager->mutexTopicos);
            if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgpRecebido.topico) == 0){
                // topico nao existe vai tentar ser criado
                if(adicionaTopico(dadosManager->arrayTopicos,&dadosManager->contadorTopicos,msgpRecebido.topico,msgpRecebido.username) == 0){
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    // nao há espaço para topico -> mandar erro
                    erroMsgEnvelope pacoteErroMsgp;
                    memset(&pacoteErroMsgp,0,sizeof(erroMsgEnvelope));
                    snprintf(pacoteErroMsgp.info.mensagemErroMsg,sizeof(pacoteErroMsgp.info.mensagemErroMsg),"[!] - Tópico %s não foi criado. Manager já não tem espaço.\n",msgRecebido.topico);
                    pacoteErroMsgp.tipo = MESSAGE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroMsgp,sizeof(pacoteErroMsgp)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    // topico foi criado, mensagem persistente vai ser armazenada nele mas topico nao vai ser subscrito
                    adicionaMensagemPersistente(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgpRecebido.mensagem,msgpRecebido.topico,msgpRecebido.tempoDeVida); // topico acabou de ser criado logo não é necessário verificar se há espaço para mensagem persistente
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    respostaMsgTopicoCriadoEnvelope pacoteRespostaMsgpTopicoCriado;
                    memset(&pacoteRespostaMsgpTopicoCriado,0,sizeof(pacoteRespostaMsgpTopicoCriado));
                    strcpy(pacoteRespostaMsgpTopicoCriado.info.topico,msgpRecebido.topico);
                    snprintf(pacoteRespostaMsgpTopicoCriado.info.mensagem,sizeof(pacoteRespostaMsgpTopicoCriado.info.mensagem),"[%s] - %s: %s\n",msgpRecebido.username,msgpRecebido.topico,msgpRecebido.mensagem);
                    pacoteRespostaMsgpTopicoCriado.tipo = MSG_CREATED_TOPIC_RESPONSE;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgpRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteRespostaMsgpTopicoCriado,sizeof(pacoteRespostaMsgpTopicoCriado)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }
            }else{
                if(verificaTopicoBloqueado(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgpRecebido.topico) == 1){
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    erroMsgEnvelope pacoteErroMsgp;
                    memset(&pacoteErroMsgp, 0, sizeof(erroMsgEnvelope));
                    snprintf(pacoteErroMsgp.info.mensagemErroMsg, sizeof(pacoteErroMsgp.info.mensagemErroMsg),"[!] - O tópico: %s está bloqueado. Mensagem não enviada.\n",msgpRecebido.topico);
                    pacoteErroMsgp.tipo = MESSAGE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgpRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroMsgp,sizeof(pacoteErroMsgp)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    respostaMsgEnvelope pacoteRespostaMsgp;
                    memset(&pacoteRespostaMsgp, 0, sizeof(respostaMsgEnvelope));
                    snprintf(pacoteRespostaMsgp.info.mensagem,sizeof(pacoteRespostaMsgp.info.mensagem),"[%s] - %s: %s\n",msgpRecebido.username,msgpRecebido.topico,msgpRecebido.mensagem);
                    pacoteRespostaMsgp.tipo = MESSAGE_RESPONSE;
                    // manda mensagem persistente para os clientes subscritos mesmo se nao a conseguir armazenar
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    for(int i = 0; i < dadosManager->contadorClientes; i++){
                        for(int j = 0; j < dadosManager->arrayClientes[i].numTopicosSubscritos; j++){
                            if(strcmp(dadosManager->arrayClientes[i].topicosSubscritos[j], msgpRecebido.topico) == 0){
                                int fdTemp = dadosManager->arrayClientes[i].pipe_fd_feed;
                                pthread_mutex_unlock(&dadosManager->mutexClientes);
                                if(write(fdTemp,&pacoteRespostaMsgp,sizeof(pacoteRespostaMsgp)) == -1){
                                    perror("Erro de escrita no pipe!\n");
                                    exit(EXIT_FAILURE);
                                }
                                pthread_mutex_lock(&dadosManager->mutexClientes);
                            }
                        }
                    }
                    // se cliente que mandou mensagem não estiver subscrito ao topico é mandada para ele a sua propria mensagem
                    if(verificaClienteSubscrito(dadosManager->arrayClientes,dadosManager->contadorClientes,msgpRecebido.topico,msgpRecebido.username) == 0){
                        int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgpRecebido.username);
                        pthread_mutex_unlock(&dadosManager->mutexClientes);
                        if(write(fdTemp,&pacoteRespostaMsgp,sizeof(pacoteRespostaMsgp)) == -1){
                            perror("Erro de escrita no pipe!\n");
                            exit(EXIT_FAILURE);
                        }
                        pthread_mutex_lock(&dadosManager->mutexClientes);
                    }
                    pthread_mutex_unlock(&dadosManager->mutexClientes);

                    pthread_mutex_lock(&dadosManager->mutexTopicos);
                    if(adicionaMensagemPersistente(dadosManager->arrayTopicos,dadosManager->contadorTopicos,msgpRecebido.mensagem,msgpRecebido.topico,msgpRecebido.tempoDeVida) == 0){
                        pthread_mutex_unlock(&dadosManager->mutexTopicos);
                        // se não existir espaço para outra mensagem persistente no topico avisa o feed que nao foi armazenada
                        erroMsgEnvelope pacoteErroMsgp;
                        memset(&pacoteErroMsgp,0,sizeof(erroMsgEnvelope));
                        snprintf(pacoteErroMsgp.info.mensagemErroMsg, sizeof(pacoteErroMsgp.info.mensagemErroMsg),"[!] - O tópico: %s não tem espaço para mais mensagens persistentes.\n",msgpRecebido.topico);
                        pacoteErroMsgp.tipo = MESSAGE_ERROR;
                        pthread_mutex_lock(&dadosManager->mutexClientes);
                        int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,msgpRecebido.username);
                        pthread_mutex_unlock(&dadosManager->mutexClientes);
                        if(write(fdTemp,&pacoteErroMsgp,sizeof(pacoteErroMsgp)) == -1){
                            perror("Erro de escrita no pipe!\n");
                            exit(EXIT_FAILURE);
                        }
                        break;
                    }else{
                        pthread_mutex_unlock(&dadosManager->mutexTopicos);
                        break;
                    }
                }
            }

        case COMMAND_TOPICS:
            read(dadosManager->pipe_manager_fd,&topicsRecebido,sizeof(topics));

            pthread_mutex_lock(&dadosManager->mutexTopicos);
            if(dadosManager->contadorTopicos == 0){
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                erroTopicsEnvelope pacoteErroTopics;
                memset(&pacoteErroTopics,0,sizeof(erroTopicsEnvelope));
                strcpy(pacoteErroTopics.info.mensagemErroTopics,"Nenhum tópico está armazenado na plataforma.\n");
                pacoteErroTopics.tipo = NO_TOPICS_AVAILABLE;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,topicsRecebido.username);
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                if(write(fdTemp,&pacoteErroTopics,sizeof(pacoteErroTopics)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                break;
            }else{
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                respostaTopicsEnvelope pacoteRespostaTopics;
                memset(&pacoteRespostaTopics,0,sizeof(respostaTopicsEnvelope));
                pthread_mutex_lock(&dadosManager->mutexTopicos);
                for(int i = 0; i < dadosManager->contadorTopicos; i++){
                    char aux[32];
                    if(dadosManager->arrayTopicos[i].locked == 0){
                        strcpy(aux,"Desbloqueado.");
                    }else{
                        strcpy(aux,"Bloqueado.");
                    }
                    snprintf(pacoteRespostaTopics.info.arrayTopics[i],sizeof(pacoteRespostaTopics.info.arrayTopics[i]),"Tópico: %s Mensagens persistentes: %d Estado: %s",dadosManager->arrayTopicos[i].nomeTopico,dadosManager->arrayTopicos[i].numMensagensPersistentes,aux);
                    pacoteRespostaTopics.info.numTopicos++;
                }
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                pacoteRespostaTopics.tipo = TOPICS_RESPONSE;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,topicsRecebido.username);
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                if(write(fdTemp,&pacoteRespostaTopics,sizeof(pacoteRespostaTopics)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                break;
            }

        case COMMAND_SUBSCRIBE:
            read(dadosManager->pipe_manager_fd,&subscribeRecebido,sizeof(subscribe));

            pthread_mutex_lock(&dadosManager->mutexTopicos);
            if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,subscribeRecebido.topico) == 0){
                //tópico não existe vai ser criado
                if(adicionaTopico(dadosManager->arrayTopicos,&dadosManager->contadorTopicos,subscribeRecebido.topico,subscribeRecebido.username) == 0){
                    // não há espaço
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    erroSubscribeEnvelope pacoteErroSubscribe;
                    memset(&pacoteErroSubscribe,0,sizeof(erroSubscribeEnvelope));
                    snprintf(pacoteErroSubscribe.info.mensagemErro,sizeof(pacoteErroSubscribe.info.mensagemErro),"[!] - Tópico %s não foi criado. Manager já não tem espaço.\n",subscribeRecebido.topico);
                    pacoteErroSubscribe.tipo = SUBSCRIBE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,subscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroSubscribe,sizeof(pacoteErroSubscribe)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    //tópico foi criado feed vai subscrever tópico acabado de criar (não recebe imediatamente msgs desse tópico porque foi acabado de criar)
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    subscribeTopico(dadosManager->arrayClientes,dadosManager->arrayTopicos,dadosManager->contadorClientes,dadosManager->contadorTopicos,subscribeRecebido.username,subscribeRecebido.topico);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    respostaSubscribeCriadoEnvelope pacoteRespostaSubscribeCriado;
                    memset(&pacoteRespostaSubscribeCriado,0,sizeof(respostaSubscribeCriadoEnvelope));
                    strcpy(pacoteRespostaSubscribeCriado.info.topico,subscribeRecebido.topico);
                    pacoteRespostaSubscribeCriado.tipo = SUBSCRIBE_CREATED_TOPIC_RESPONSE;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,subscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteRespostaSubscribeCriado,sizeof(pacoteRespostaSubscribeCriado)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }
            }else{
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                pthread_mutex_lock(&dadosManager->mutexClientes);
                if(verificaClienteSubscrito(dadosManager->arrayClientes,dadosManager->contadorClientes,subscribeRecebido.topico,subscribeRecebido.username) == 1){
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    // cliente já está subscrito no tópico
                    erroSubscribeEnvelope pacoteErroSubscribe;
                    memset(&pacoteErroSubscribe,0,sizeof(erroSubscribeEnvelope));
                    snprintf(pacoteErroSubscribe.info.mensagemErro,sizeof(pacoteErroSubscribe.info.mensagemErro),"[!] - Já se encontra subscrito ao tópico %s.\n",subscribeRecebido.topico);
                    pacoteErroSubscribe.tipo = SUBSCRIBE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,subscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroSubscribe,sizeof(pacoteErroSubscribe)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    // topico existe feed vai subscreve-lo
                    pthread_mutex_lock(&dadosManager->mutexTopicos);
                    subscribeTopico(dadosManager->arrayClientes,dadosManager->arrayTopicos,dadosManager->contadorClientes,dadosManager->contadorTopicos,subscribeRecebido.username,subscribeRecebido.topico);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    respostaSubscribeEnvelope pacoteRespostaSubscribe;
                    memset(&pacoteRespostaSubscribe, 0, sizeof(respostaSubscribeEnvelope));
                    strcpy(pacoteRespostaSubscribe.info.topico, subscribeRecebido.topico);
                    // o utilizador recebe de imediato todas as mensagens persistentes desse tópico
                    pthread_mutex_lock(&dadosManager->mutexTopicos);
                    for(int i = 0; i < dadosManager->contadorTopicos; i++){
                        if(strcmp(dadosManager->arrayTopicos[i].nomeTopico, subscribeRecebido.topico) == 0){
                            for(int j = 0; j < dadosManager->arrayTopicos[i].numMensagensPersistentes; j++){
                                strcpy(pacoteRespostaSubscribe.info.arrayMensagensPersistentes[j],dadosManager->arrayTopicos[i].mensagensPersistentes[j]);
                                pacoteRespostaSubscribe.info.numMensagensPersistentes++;
                            }
                        }
                    }
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    pacoteRespostaSubscribe.tipo = SUBSCRIBE_RESPONSE;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,subscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteRespostaSubscribe,sizeof(pacoteRespostaSubscribe)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }
            }

        case COMMAND_UNSUBSCRIBE:
            read(dadosManager->pipe_manager_fd,&unsubscribeRecebido,sizeof(unsubscribe));

            pthread_mutex_lock(&dadosManager->mutexTopicos);
            if(verificaTopicoRepetido(dadosManager->arrayTopicos,dadosManager->contadorTopicos,unsubscribeRecebido.topico) == 0){
                //topico nao existe
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                erroUnsubscribeEnvelope pacoteErroUnsubscribe;
                memset(&pacoteErroUnsubscribe,0,sizeof(erroUnsubscribeEnvelope));
                snprintf(pacoteErroUnsubscribe.info.mensagemErro,sizeof(pacoteErroUnsubscribe.info.mensagemErro),"Tópico %s introduzido não se encontra registado no manager.\n",unsubscribeRecebido.topico);
                pacoteErroUnsubscribe.tipo = UNSUBSCRIBE_ERROR;
                pthread_mutex_lock(&dadosManager->mutexClientes);
                int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,unsubscribeRecebido.username);
                pthread_mutex_unlock(&dadosManager->mutexClientes);
                if(write(fdTemp,&pacoteErroUnsubscribe,sizeof(pacoteErroUnsubscribe)) == -1){
                    perror("Erro de escrita no pipe!\n");
                    exit(EXIT_FAILURE);
                }
                break;
            }else{
                pthread_mutex_unlock(&dadosManager->mutexTopicos);
                pthread_mutex_lock(&dadosManager->mutexClientes);
                if(verificaClienteSubscrito(dadosManager->arrayClientes,dadosManager->contadorClientes,unsubscribeRecebido.topico,unsubscribeRecebido.username) == 0){
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    erroUnsubscribeEnvelope pacoteErroUnsubscribe;
                    memset(&pacoteErroUnsubscribe,0,sizeof(erroUnsubscribeEnvelope));
                    snprintf(pacoteErroUnsubscribe.info.mensagemErro,sizeof(pacoteErroUnsubscribe.info.mensagemErro),"Não se encontra subscrito ao topico %s.\n",unsubscribeRecebido.topico);
                    pacoteErroUnsubscribe.tipo = UNSUBSCRIBE_ERROR;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,unsubscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteErroUnsubscribe,sizeof(pacoteErroUnsubscribe)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }else{
                    //topico existe vai ser cancelada subscrição
                    pthread_mutex_lock(&dadosManager->mutexTopicos);
                    unsubscribeTopico(dadosManager->arrayClientes,dadosManager->arrayTopicos,dadosManager->contadorClientes,dadosManager->contadorTopicos,unsubscribeRecebido.username, unsubscribeRecebido.topico);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    pthread_mutex_unlock(&dadosManager->mutexTopicos);
                    // aqui thread deve detetar se topico já não tem clientes subscritos
                    respostaUnsubscribeEnvelope pacoteRespostaUnsubscribe;
                    memset(&pacoteRespostaUnsubscribe, 0, sizeof(respostaUnsubscribeEnvelope));
                    strcpy(pacoteRespostaUnsubscribe.info.topico, unsubscribeRecebido.topico);
                    pacoteRespostaUnsubscribe.tipo = UNSUBSCRIBE_RESPONSE;
                    pthread_mutex_lock(&dadosManager->mutexClientes);
                    int fdTemp = devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,unsubscribeRecebido.username);
                    pthread_mutex_unlock(&dadosManager->mutexClientes);
                    if(write(fdTemp,&pacoteRespostaUnsubscribe,sizeof(pacoteRespostaUnsubscribe)) == -1){
                        perror("Erro de escrita no pipe!\n");
                        exit(EXIT_FAILURE);
                    }
                    break;
                }
            }

        case COMMAND_EXIT:
            read(dadosManager->pipe_manager_fd,&EXITRecebido,sizeof(EXIT));
            pthread_mutex_lock(&dadosManager->mutexClientes);
            close(devolveFdFeed(dadosManager->arrayClientes,dadosManager->contadorClientes,EXITRecebido.username));
            pthread_mutex_lock(&dadosManager->mutexTopicos);
            eliminaCliente(dadosManager->arrayClientes,dadosManager->arrayTopicos,&dadosManager->contadorClientes,&dadosManager->contadorTopicos,EXITRecebido.username);
            pthread_mutex_unlock(&dadosManager->mutexTopicos);
            pthread_mutex_unlock(&dadosManager->mutexClientes);
            printf("O utilizador %s saiu da plataforma.\n",EXITRecebido.username);
            break;

        case FEED_HEARTBEAT:
            read(dadosManager->pipe_manager_fd,&heartbeatRecebido,sizeof(heartbeat));
            pthread_mutex_lock(&dadosManager->mutexClientes);
            pthread_mutex_lock(&dadosManager->mutexTime);
            atualizaHeartbeat(dadosManager->arrayClientes,dadosManager->contadorClientes,heartbeatRecebido.username);
            pthread_mutex_unlock(&dadosManager->mutexTime);
            pthread_mutex_unlock(&dadosManager->mutexClientes);
            break;

        default:
            break;

    }
}

int main(){

    ManagerData dadosManager;
    memset(&dadosManager,0,sizeof(ManagerData));

    struct sigaction sa;
    memset(&sa,0,sizeof(sa));
    sa.sa_handler = trata_sigint;

    if(sigaction(SIGINT,&sa,NULL) == -1){
        perror("Erro ao configurar sigaction.\n");
        exit(EXIT_FAILURE);
    }

    dadosManager.ficheiroMensagens = getenv("MSG_FICH");

    if(dadosManager.ficheiroMensagens == NULL){
        printf("[!] - A variável de ambiente MSG_FICH não está definida.\n");
        exit(EXIT_FAILURE);
    }
    printf("Ficheiro para mensagens persistentes definido como: %s\n",dadosManager.ficheiroMensagens);

    fd_set read_fds;
    int max_fd;
    int tipo;
    char inputBuffer[INPUT_BUFFER_MAX];

    // cria o pipe do manager
    if(mkfifo(PIPE_NAME_MANAGER,0666) == -1){
        printf("[!] - Erro ao criar o named pipe: manager a correr.\n");
        exit(EXIT_FAILURE);
    }

    dadosManager.pipe_manager_fd = open(PIPE_NAME_MANAGER,O_RDWR);
    if(dadosManager.pipe_manager_fd == -1){
        printf("[!] - Erro ao abrir o named pipe do manager");
        unlink(PIPE_NAME_MANAGER);
        exit(EXIT_FAILURE);
    }

    // protege flags de paragem das threads
    pthread_mutex_init(&dadosManager.mutexStop,NULL);
    // protege temporização do heartbeat
    pthread_mutex_init(&dadosManager.mutexTime,NULL);
    // protege topicos armazenados no manager
    pthread_mutex_init(&dadosManager.mutexTopicos,NULL);
    // protege clientes armazenados no manager
    pthread_mutex_init(&dadosManager.mutexClientes,NULL);

    dadosManager.threadParamsTempos.arrayTopicos = dadosManager.arrayTopicos;
    dadosManager.threadParamsTempos.contadorTopicos = &dadosManager.contadorTopicos;
    dadosManager.threadParamsTempos.stop = 0;
    dadosManager.threadParamsTempos.mutexTopicos = dadosManager.mutexTopicos;

    dadosManager.threadParamsHeartbeat.arrayClientes = dadosManager.arrayClientes;
    dadosManager.threadParamsHeartbeat.contadorClientes = &dadosManager.contadorClientes;
    dadosManager.threadParamsHeartbeat.arrayTopicos = dadosManager.arrayTopicos;
    dadosManager.threadParamsHeartbeat.contadorTopicos = &dadosManager.contadorTopicos;
    dadosManager.threadParamsHeartbeat.stop = 0;
    dadosManager.threadParamsHeartbeat.stopThreadDecrementa = &dadosManager.threadParamsTempos.stop; // passa para thread de heartbeat a flag de paragem da thread decrementa
    dadosManager.threadParamsHeartbeat.fd_manager = dadosManager.pipe_manager_fd;
    dadosManager.threadParamsHeartbeat.heartbeat = MANAGER_HEARTBEAT;
    dadosManager.threadParamsHeartbeat.pacoteCLOSE.tipo = MANAGER_CLOSE;
    strcpy(dadosManager.threadParamsHeartbeat.pacoteCLOSE.info.mensagemSaida,"Manager foi encerrado.\n");
    dadosManager.threadParamsHeartbeat.mutexTopicos = dadosManager.mutexTopicos;
    dadosManager.threadParamsHeartbeat.mutexClientes = dadosManager.mutexClientes;
    dadosManager.threadParamsHeartbeat.mutexStop = dadosManager.mutexStop;
    dadosManager.threadParamsHeartbeat.mutexTime = dadosManager.mutexTime;
    dadosManager.threadParamsHeartbeat.ficheiroMensagens = dadosManager.ficheiroMensagens;

    recuperarMensagensPersistentes(dadosManager.arrayTopicos,&dadosManager.contadorTopicos,dadosManager.ficheiroMensagens);

    if(pthread_create(&dadosManager.threadHeartbeat,NULL,managerHeartbeat,(void *) &dadosManager.threadParamsHeartbeat) != 0){
        perror("[!] - Erro ao criar a thread.\n");
        exit(EXIT_FAILURE);
    }

    if(pthread_create(&dadosManager.threadTemposDeVida,NULL,decrementaTemposDeVida,(void *)&dadosManager.threadParamsTempos) != 0){
        perror("[!] - Erro ao criar a thread.\n");
        exit(EXIT_FAILURE);
    }

    printf("Manager online. Escreva 'commands' para uma lista de comandos disponíveis.\n");

    while(1){

        //SELECT LOOP

        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO,&read_fds);
        FD_SET(dadosManager.pipe_manager_fd,&read_fds);
        max_fd = (STDIN_FILENO > dadosManager.pipe_manager_fd) ? STDIN_FILENO : dadosManager.pipe_manager_fd; // operador ternário

        int ret = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if(ret == -1){
            perror("[!] - Erro no select.\n");
            break;
        }

        // Verifica se há dados disponíveis para leitura no named pipe
        if(FD_ISSET(dadosManager.pipe_manager_fd,&read_fds)){
            ssize_t bytesLidos = read(dadosManager.pipe_manager_fd,&tipo,sizeof(tipo));
            if(bytesLidos > 0){
                processaPipe(tipo,&dadosManager);
            }else if(bytesLidos == -1){
                perror("[!] - Erro ao ler do pipe do manager.\n");
                break;
            }
        }

        // Verifica se há dados disponíveis para leitura no stdin
        if(FD_ISSET(STDIN_FILENO,&read_fds)){
            ssize_t bytesLidos = read(STDIN_FILENO,inputBuffer,INPUT_BUFFER_MAX - 1);
            if(bytesLidos > 0){
                processaStdin(inputBuffer,&dadosManager);
            }else if(bytesLidos == -1){
                perror("[!] - Erro ao ler do stdin.\n");
                break;
            }
        }
    }
    // se houver um break so ciclo do select salta para aqui (POR EXEMPLO RECEBE UM SIGINT)
    CLOSEenvelope pacoteCLOSE;
    memset(&pacoteCLOSE,0,sizeof(CLOSEenvelope));
    pacoteCLOSE.tipo = MANAGER_CLOSE;
    strcpy(pacoteCLOSE.info.mensagemSaida,"Manager foi encerrado.\n");
    pthread_mutex_lock(&dadosManager.mutexClientes);
    for(int i = 0; i < dadosManager.contadorClientes; i++){
        pthread_mutex_unlock(&dadosManager.mutexClientes);
        if(write(dadosManager.arrayClientes[i].pipe_fd_feed,&pacoteCLOSE,sizeof(pacoteCLOSE)) == -1){
            perror("Erro de escrita no pipe!\n");
            exit(EXIT_FAILURE);
        }
        pthread_mutex_lock(&dadosManager.mutexClientes);
        close(dadosManager.arrayClientes[i].pipe_fd_feed);
    }
    pthread_mutex_unlock(&dadosManager.mutexClientes);
    pthread_mutex_lock(&dadosManager.mutexStop);
    dadosManager.threadParamsTempos.stop = 1;
    dadosManager.threadParamsHeartbeat.stop = 1;
    pthread_mutex_unlock(&dadosManager.mutexStop);
    pthread_join(dadosManager.threadTemposDeVida,NULL);
    pthread_join(dadosManager.threadHeartbeat,NULL);
    close(dadosManager.pipe_manager_fd);
    unlink(PIPE_NAME_MANAGER);
    pthread_mutex_lock(&dadosManager.mutexTopicos);
    salvarMensagensPersistentes(dadosManager.arrayTopicos,dadosManager.contadorTopicos,dadosManager.ficheiroMensagens);
    pthread_mutex_unlock(&dadosManager.mutexTopicos);
    pthread_mutex_destroy(&dadosManager.mutexTopicos);
    pthread_mutex_destroy(&dadosManager.mutexClientes);
    pthread_mutex_destroy(&dadosManager.mutexTime);
    pthread_mutex_destroy(&dadosManager.mutexStop);
    printf("A encerrar manager...\n");
    exit(0);
}