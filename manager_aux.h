#ifndef TRABALHO_MANAGER_AUX_H
#define TRABALHO_MANAGER_AUX_H

int adicionaCliente(armazenamentoClientes arrayClientes[MAX_CLIENTES],int *contadorClientes,const char *username,int fd);
int eliminaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorTopicos,const char *topico);
int eliminaCliente(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorClientes,int *contadorTopicos,const char *username);
int adicionaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int *contadorTopicos,const char *topico,const char *autor);
int adicionaMensagemPersistente(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *mensagemPersistente,const char *topico,int tempoDeVida);
int verificaUsernameRepetido(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username);
int verificaTopicoRepetido(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico);
void listaUtilizadoresNaPlataforma(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes);
void listaTopicosNaPlataforma(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos);
void listaMensagensPersistentes(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico);
int devolveFdFeed(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username);
int subscribeTopico(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorClientes,int contadorTopicos,const char *username,const char *topico);
int unsubscribeTopico(armazenamentoClientes arrayClientes[MAX_CLIENTES],armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorClientes,int contadorTopicos,const char *username,const char *topico);
int bloqueiaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico);
int desbloqueiaTopico(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico);
int verificaTopicoBloqueado(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *topico);
int verificaClienteSubscrito(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *topico,const char *username);
void atualizaHeartbeat(armazenamentoClientes arrayClientes[MAX_CLIENTES],int contadorClientes,const char *username);
void salvarMensagensPersistentes(armazenamentoTopicos arrayTopicos[MAX_TOPICOS],int contadorTopicos,const char *ficheiro);
void recuperarMensagensPersistentes(armazenamentoTopicos arrayTopicos[20], int *contadorTopicos, const char *ficheiro);

#endif