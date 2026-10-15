#include "manager_data.h"
#include "shared.h"
#include "manager_aux.h"

int adicionaCliente(armazenamentoClientes arrayClientes[MAX_CLIENTES],int *contadorClientes,const char *username,int fd){
    if(*contadorClientes >= MAX_CLIENTES){
        printf("[!] - Cliente não foi adicionado! Capacidade máxima atingida.\n");
        return 0;
    }
    strcpy(arrayClientes[*contadorClientes].nomeCliente,username);
    arrayClientes[*contadorClientes].pipe_fd_feed = fd;
    arrayClientes[*contadorClientes].numTopicosSubscritos = 0;
    arrayClientes[*contadorClientes].lastTimeHeartbeat = getTimeMilliseconds();
    (*contadorClientes)++;
    return 1;
}

int eliminaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorTopicos,const char *topico){
    for(int i = 0; i < *contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            //memset(&arrayTopicos[i],0,sizeof(armazenamentoTopicos));

            for(int j = i; j < *contadorTopicos; j++){
                arrayTopicos[j] = arrayTopicos[j + 1];
            }

            //memset(&arrayTopicos[*contadorTopicos],0,sizeof(armazenamentoTopicos));

            (*contadorTopicos)--;
            return 0;
        }
    }
    return -1; // topico não encontrado (em principio nao deve chegar aqui)
}

int eliminaCliente(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorClientes,int *contadorTopicos,const char *username){
    for(int i = 0; i < *contadorClientes; i++){ // percorre clientes
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){ // cliente a eliminar
            for(int k = 0; k < arrayClientes[i].numTopicosSubscritos; k++){ // percorre topicos subscritos
                for(int y = 0; y < *contadorTopicos; y++){ // percorre topicos
                    if(strcmp(arrayClientes[i].topicosSubscritos[k],arrayTopicos[y].nomeTopico) == 0){ // se topico subscrito for igual
                        arrayTopicos[y].clientesSubscritos--; // decrementa numero de clientes subscritos
                        if(arrayTopicos[y].clientesSubscritos == 0 && arrayTopicos[y].numMensagensPersistentes == 0){
                            eliminaTopico(arrayTopicos,contadorTopicos,arrayTopicos[y].nomeTopico);
                        }
                    }
                }
            }
            // fecha e remove pipe
            close(arrayClientes[i].pipe_fd_feed);
            char feedPipeName[TAM_FEED_PIPE_NAME];
            snprintf(feedPipeName,sizeof(feedPipeName),"pipe_feed_%s",arrayClientes[i].nomeCliente);
            unlink(feedPipeName);
            // a partir daqui remove cliente

            // move todos os clientes subsequentes para a esquerda
            for(int j = i; j < *contadorClientes; j++){
                arrayClientes[j] = arrayClientes[j + 1];
            }

            (*contadorClientes)--; // atualiza contador
            return 0;
        }
    }
    return -1; // cliente não encontrado (em principio nao deve chegar aqui)
}

int adicionaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorTopicos,const char *topico,const char *autor){
    if(*contadorTopicos >= MAX_TOPICOS){
        printf("[!] - Tópico não adicionado! Capacidade máxima atingida.\n");
        return 0;
    }
    arrayTopicos[*contadorTopicos].locked = 0;
    strcpy(arrayTopicos[*contadorTopicos].nomeTopico,topico);
    strcpy(arrayTopicos[*contadorTopicos].nomeAutor,autor);
    arrayTopicos[*contadorTopicos].numMensagensPersistentes = 0;
    (*contadorTopicos)++;
    return 1;
}

int adicionaMensagemPersistente(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *mensagemPersistente,const char *topico,int tempoDeVida){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            if(arrayTopicos[i].numMensagensPersistentes >= MAX_MSGP){
                printf("[!] - Mensagem persistente não foi adicionada ao tópico! Capacidade máxima atingida.\n");
                return 0;
            }else{
                strcpy(arrayTopicos[i].mensagensPersistentes[arrayTopicos[i].numMensagensPersistentes],mensagemPersistente);
                arrayTopicos[i].temposDeVida[arrayTopicos[i].numMensagensPersistentes] = tempoDeVida;
                arrayTopicos[i].numMensagensPersistentes++;
                return 1;
            }
        }
    }
    return -1;
}

int verificaUsernameRepetido(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            return 1; // username ja existe
        }
    }
    return 0;
}

int verificaTopicoRepetido(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            return 1; // topico ja existe
        }
    }
    return 0;
}

void listaUtilizadoresNaPlataforma(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes){
    if(contadorClientes == 0){
        printf("Nenhum utilizador online.\n");
        return;
    }
    for(int i = 0; i < contadorClientes; i++){
        printf("%d -> %s\n",i+1,arrayClientes[i].nomeCliente);
    }
}

void listaTopicosNaPlataforma(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos){
    if(contadorTopicos == 0){
        printf("Nenhum tópico registado na plataforma.\n");
        return;
    }
    for(int i = 0; i < contadorTopicos; i++){
        printf("Tópico: %s tem %d mensagem(s) persistente(s).\n",arrayTopicos[i].nomeTopico,arrayTopicos[i].numMensagensPersistentes);
    }
}

void listaMensagensPersistentes(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            if(arrayTopicos[i].numMensagensPersistentes == 0){
                printf("Tópico %s não tem mensagens persistentes.\n",topico);
                return;
            }else{
                for(int j = 0; j < arrayTopicos[i].numMensagensPersistentes; j++){
                    printf("%s -> Tempo de vida restante: %d segundos.\n",arrayTopicos[i].mensagensPersistentes[j],arrayTopicos[i].temposDeVida[j]);
                }
            }
        }
    }
}

int devolveFdFeed(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            return arrayClientes[i].pipe_fd_feed;
        }
    }
    return -1;
}

int subscribeTopico(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorClientes,int contadorTopicos,const char *username,const char *topico){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            strcpy(arrayClientes[i].topicosSubscritos[arrayClientes[i].numTopicosSubscritos],topico);
            arrayClientes[i].numTopicosSubscritos++;
        }
    }
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            arrayTopicos[i].clientesSubscritos++;
        }
    }
    return 0;
}

int unsubscribeTopico(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorClientes,int contadorTopicos,const char *username,const char *topico){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            for(int j = 0; j < arrayClientes[i].numTopicosSubscritos; j++){
                if(strcmp(arrayClientes[i].topicosSubscritos[j],topico) == 0){
                    //memset(&arrayClientes[i].topicosSubscritos[j],0,TAM_TOPICO); // limpa topico

                    // move todos os topicos subsequentes para a esquerda
                    for(int k = j; k < arrayClientes[i].numTopicosSubscritos; k++){
                        strcpy(arrayClientes[i].topicosSubscritos[k],arrayClientes[i].topicosSubscritos[k + 1]);
                    }

                    //memset(arrayClientes[i].topicosSubscritos[arrayClientes[i].numTopicosSubscritos],0,TAM_TOPICO); // Limpa a última posição do array agora redundante

                    arrayClientes[i].numTopicosSubscritos--;
                }
            }
        }
    }
    for(int i = 0;i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0){
            arrayTopicos[i].clientesSubscritos--;
        }
    }
    return 0;
}

int bloqueiaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0 && arrayTopicos[i].locked == 0){
            arrayTopicos[i].locked = 1;
            return 1;
        }
    }
    return 0; // se devolve 0 é porque o topico já está bloqueado
}

int desbloqueiaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0 && arrayTopicos[i].locked == 1){
            arrayTopicos[i].locked = 0;
            return 1;
        }
    }
    return 0; // se devolve 0 é porque o topico já está desbloqueado
}

int verificaTopicoBloqueado(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico){
    for(int i = 0; i < contadorTopicos; i++){
        if(strcmp(arrayTopicos[i].nomeTopico,topico) == 0 && arrayTopicos[i].locked == 1){
            return 1;
        }
    }
    return 0;
}

int verificaClienteSubscrito(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *topico,const char *username){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            for(int j = 0; j < arrayClientes[i].numTopicosSubscritos; j++){
                if(strcmp(arrayClientes[i].topicosSubscritos[j], topico) == 0){
                    return 1; // cliente encontra-se subscrito ao topico
                }
            }
            return 0; // cliente não está subscrito ao topico
        }
    }
    return -1; // em principio nao chega aqui (já verificou antes se cliente existe)
}

void atualizaHeartbeat(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username){
    for(int i = 0; i < contadorClientes; i++){
        if(strcmp(arrayClientes[i].nomeCliente,username) == 0){
            arrayClientes[i].lastTimeHeartbeat = getTimeMilliseconds();
        }
    }
}

void salvarMensagensPersistentes(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *ficheiro){
    FILE *file = fopen(ficheiro,"w"); // abre para escrita
    if(!file){
        perror("[!] - Erro ao abrir o ficheiro para escrita");
        return;
    }

    for(int i = 0; i < contadorTopicos; i++){
        for(int j = 0; j < arrayTopicos[i].numMensagensPersistentes; j++){
            // escreve uma linha no ficheiro de texto
            fprintf(file,"%s %s %d %s\n",arrayTopicos[i].nomeTopico,arrayTopicos[i].nomeAutor,arrayTopicos[i].temposDeVida[j],arrayTopicos[i].mensagensPersistentes[j]);
        }
    }

    fclose(file);
}

void recuperarMensagensPersistentes(armazenamentoTopicos arrayTopicos[20], int *contadorTopicos, const char *ficheiro){
    FILE *file = fopen(ficheiro,"r"); // abre para leitura
    if(!file){
        printf("Ficheiro '%s' não encontrado. Nenhuma mensagem persistente foi carregada.\n",ficheiro);
        return;
    }

    char linha[512];
    while(fgets(linha,sizeof(linha),file)){ // lê uma linha de texto
        char topico[TAM_TOPICO];
        char autor[TAM_USER];
        char tempoString[32];
        char mensagem[TAM_MSG];

        // divide linha lida em pedaços (tokens)
        char *token = strtok(linha," ");
        strcpy(topico,token);
        token = strtok(NULL," ");
        strcpy(autor,token);
        token = strtok(NULL," ");
        strcpy(tempoString,token);
        token = strtok(NULL,"\n");
        strcpy(mensagem,token);

        char *endptr;
        long tempoDeVidaLong = strtol(tempoString,&endptr,10); // conversão string para long
        int tempoDeVida =(int)tempoDeVidaLong; // cast para int

        // guarda informações da linha do ficheiro nas estruturas
        if(verificaTopicoRepetido(arrayTopicos,*contadorTopicos,topico) == 0){
            adicionaTopico(arrayTopicos,contadorTopicos,topico,autor);
        }
        adicionaMensagemPersistente(arrayTopicos,*contadorTopicos,mensagem,topico,tempoDeVida);

    }
    fclose(file);
}