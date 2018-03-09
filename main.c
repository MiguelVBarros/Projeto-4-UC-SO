/**************************************************
******	  Projeto de Sistemas Operativos 	*******
******				 Grupo 57				*******
******										*******
******	     		Exercício 4				*******
**************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <string.h>



#include "matrix2d.h"
#include "util.h"


#define UNUSED -2

/*--------------------------------------------------------------------
| Type: thread_info
| Description: Estrutura com Informacao para Trabalhadoras
---------------------------------------------------------------------*/

typedef struct {
	int    id;
	int    N;
	int    iter;
	int    trab;
	int    tam_fatia;
	double maxD;
} thread_info;

/*--------------------------------------------------------------------
| Type: doubleBarrierWithMax
| Description: Barreira dupla com variavel de max-reduction
---------------------------------------------------------------------*/

typedef struct {
	int             total_nodes;
	int 						exit_flag;
	int 						exit_nodes;
	int             pending[2];
	double          maxdelta[2];
	int             iteracoes_concluidas;
	pthread_mutex_t mutex;
	pthread_cond_t  wait[2];
} DualBarrierWithMax;

/*--------------------------------------------------------------------
| Global variables
---------------------------------------------------------------------*/

DoubleMatrix2D     *matrix_copies[2];
DualBarrierWithMax *dual_barrier;
double              maxD;
int periodoS;

char * fichS;
char *aux_fich;

int N;
int should_I_continue = 1;
int pid_currently_saving = UNUSED;
int recebi_alarm;


/*--------------------------------------------------------------------
| Function: trataCtrlC_main
| Description: Ativa a flag que indica ao HeatSim para terminar e 
|							 guardar o seu estado
---------------------------------------------------------------------*/
void trataCtrlC_main (int s) {
	// Dizer às thread para não fazerem mais -> flag a zero
	if (pthread_mutex_lock(&dual_barrier->mutex) != 0) {
		fprintf(stderr, "\nErro a bloquear mutex\n");
		exit(EXIT_FAILURE);
	}

	//dizemos as threads que deviam parar de iterar
	should_I_continue = 0;

	if (pthread_mutex_unlock(&dual_barrier->mutex) != 0) {
		fprintf(stderr, "\nErro ao desbloquear mutex\n");
		exit(EXIT_FAILURE);
	}
}


/*--------------------------------------------------------------------
| Function: trata_alarm
| Description: Ativa a flag para fazer a salvaguarda
---------------------------------------------------------------------*/
void trata_alarm (int s) {
	if (pthread_mutex_lock(&dual_barrier->mutex) != 0) {
		fprintf(stderr, "\nErro a bloquear mutex\n");
		exit(EXIT_FAILURE);
	}
	//flag que indica o periodoS para guardar a matriz
	recebi_alarm = 1;

	if (pthread_mutex_unlock(&dual_barrier->mutex) != 0) {
		fprintf(stderr, "\nErro ao desbloquear mutex\n");
		exit(EXIT_FAILURE);
	}
	alarm (periodoS);
}




/*--------------------------------------------------------------------
| Function: dualBarrierInit
| Description: Inicializa uma barreira dupla
---------------------------------------------------------------------*/
DualBarrierWithMax *dualBarrierInit(int ntasks) {
	DualBarrierWithMax *b;
	b = (DualBarrierWithMax*) malloc (sizeof(DualBarrierWithMax));
	if (b == NULL) return NULL;
	b->exit_flag = 0;
	b->exit_nodes = 0;
	b->total_nodes = ntasks;
	b->pending[0]  = ntasks;
	b->pending[1]  = ntasks;
	b->maxdelta[0] = 0;
	b->maxdelta[1] = 0;
	b->iteracoes_concluidas = 0;

	if (pthread_mutex_init(&(b->mutex), NULL) != 0) {
		fprintf(stderr, "\nErro a inicializar mutex\n");
		exit(EXIT_FAILURE);
	}
	if (pthread_cond_init(&(b->wait[0]), NULL) != 0) {
		fprintf(stderr, "\nErro a inicializar variável de condição\n");
		exit(EXIT_FAILURE);
	}
	if (pthread_cond_init(&(b->wait[1]), NULL) != 0) {
		fprintf(stderr, "\nErro a inicializar variável de condição\n");
		exit(EXIT_FAILURE);
	}
	return b;
}

/*--------------------------------------------------------------------
| Function: dualBarrierFree
| Description: Liberta os recursos de uma barreira dupla
---------------------------------------------------------------------*/
void dualBarrierFree(DualBarrierWithMax* b) {
	if (pthread_mutex_destroy(&(b->mutex)) != 0) {
		fprintf(stderr, "\nErro a destruir mutex\n");
		exit(EXIT_FAILURE);
	}
	if (pthread_cond_destroy(&(b->wait[0])) != 0) {
		fprintf(stderr, "\nErro a destruir variável de condição\n");
		exit(EXIT_FAILURE);
	}
	if (pthread_cond_destroy(&(b->wait[1])) != 0) {
		fprintf(stderr, "\nErro a destruir variável de condição\n");
		exit(EXIT_FAILURE);
	}
	free(b);
}


/*--------------------------------------------------------------------
| Function: salvaguarda
| Description: Guarda o conteudo de uma matriz para um ficheiro
---------------------------------------------------------------------*/
void salvaguarda(int current) {

	FILE *f = fopen(aux_fich, "w");
	if (f == NULL)
		die ("Erro ao abrir ficheiro");

	if ((writeMatrix2dToFile(f, matrix_copies[current], N + 2, N + 2)) == -1) {
		fclose(f);
		die ("Erro ao salvaguardar matriz");
	}

	fclose(f);
	
	if (rename(aux_fich, fichS) == -1) {
		fprintf(stderr, "\nErro ao mudar o nome dos ficheiros\n");
		exit(EXIT_FAILURE);
	}

	exit(EXIT_SUCCESS);
}

/*--------------------------------------------------------------------
| Function: dualBarrierWait
| Description: Ao chamar esta funcao, a tarefa fica bloqueada ate que
|              o numero 'ntasks' de tarefas necessario tenham chamado
|              esta funcao, especificado ao ininializar a barreira em
|              dualBarrierInit(ntasks). Esta funcao tambem calcula o
|              delta maximo entre todas as threads e devolve o
|              resultado no valor de retorno e lê as flags
|							 que lhe dizem se eve terminar e se deve 
|							 salvaguardar a matriz
---------------------------------------------------------------------*/

double dualBarrierWait (DualBarrierWithMax* b, int current, double localmax) {
	int next = 1 - current;
	if (pthread_mutex_lock(&(b->mutex)) != 0) {
		fprintf(stderr, "\nErro a bloquear mutex\n");
		exit(EXIT_FAILURE);
	}
	// decrementar contador de tarefas restantes
	b->pending[current]--;
	// actualizar valor maxDelta entre todas as threads
	if (b->maxdelta[current] < localmax)
		b->maxdelta[current] = localmax;
	// verificar se sou a ultima tarefa
	if (b->pending[current] == 0) {
		// sim -- inicializar proxima barreira e libertar threads

		if (recebi_alarm) {

			// que ja houve outro processo a guardar e o pid desse
			// e pid_currently_saving
			if (pid_currently_saving > 0) {
				int wait_value = waitpid(pid_currently_saving, NULL, WNOHANG);

				//quer dizer que ja acabou de guardar 
				if (wait_value > 0) {

					if ((pid_currently_saving = fork()) == -1)
						die ("Erro no fork");

					if (pid_currently_saving == 0)
						salvaguarda(current);
				}

				else if (wait_value == -1)
					die("Erro ao fazer waitpid");
			}

			// primeira vez que vamos fazer a salvaguarda 
			else if (pid_currently_saving == UNUSED) {
				pid_currently_saving = fork();

				// o que o processo filho deve fazer
				if (pid_currently_saving == 0) {
					salvaguarda(current);
				}
			}


			//Como dito na FAQ caso haja um erro na fork queremos simplesmente nao 
			//Fazer a salvaguarda
			//if (pid_currently_saving == -1)
				//die("Erro ao fazer fork");

			//reset da flag
			recebi_alarm = 0;
		}


		b->iteracoes_concluidas++;

		b->pending[next]  = b->total_nodes;
		b->maxdelta[next] = 0;


		//indica a todas as threads que um Ctrl+C aconteceu e que deviam parar de iterar
		if (!should_I_continue)
			b->exit_flag = 1;

		if (pthread_cond_broadcast(&(b->wait[current])) != 0) {
			fprintf(stderr, "\nErro a assinalar todos em variável de condição\n");
			exit(EXIT_FAILURE);
		}
	}

	else {
		// nao -- esperar pelas outras tarefas ou sair
		//if(!should_I_continue)
		//pthread_exit(0);
		while (b->pending[current] > 0) {
			if (pthread_cond_wait(&(b->wait[current]), &(b->mutex)) != 0) {
				fprintf(stderr, "\nErro a esperar em variável de condição\n");
				exit(EXIT_FAILURE);
			}
		}
	}
	double maxdelta = b->maxdelta[current];

	//Caso tenha havido um ctrlC nesta iteracao ira terminar a execucao guardando para o ficheiro
	if (b->exit_flag) {
		b->exit_nodes++;
		if (b->exit_nodes == b->total_nodes) {

			//ultimo a sair
			if (pid_currently_saving != UNUSED) {

				//desta vez queremos mesmo esperar
				//verificamos se ja guardamos alguma vez e se estivermos a guardar esperamos que acabe
				int wait_value = waitpid(pid_currently_saving, NULL, 0);

				if (wait_value == -1)
					die("Erro ao fazer waitpid");

			}

			int pid = fork();
			if (pid == 0)

				//processo filho
				salvaguarda(current);

			//processo pai
			if (pid == -1)
				die("Erro ao fazer fork");

			pid_currently_saving = pid;
		}
		if (pthread_mutex_unlock(&(b->mutex)) != 0) {
			fprintf(stderr, "\nErro a desbloquear mutex\n");
			exit(EXIT_FAILURE);
		}
		pthread_exit(EXIT_SUCCESS);
	}
	if (pthread_mutex_unlock(&(b->mutex)) != 0) {
		fprintf(stderr, "\nErro a desbloquear mutex\n");
		exit(EXIT_FAILURE);
	}

	return maxdelta;
}

/*--------------------------------------------------------------------
| Function: tarefa_trabalhadora
| Description: Funcao executada por cada tarefa trabalhadora.
|              Recebe como argumento uma estrutura do tipo thread_info
---------------------------------------------------------------------*/
void *tarefa_trabalhadora(void *args) {
	thread_info *tinfo = (thread_info *) args;
	int tam_fatia = tinfo->tam_fatia;
	int my_base = tinfo->id * tam_fatia;
	double global_delta = INFINITY;
	int iter = 0;
	do {
		int atual = iter % 2;
		int prox = 1 - iter % 2;
		double max_delta = 0;

		// Calcular Pontos Internos
		for (int i = my_base; i < my_base + tinfo->tam_fatia; i++) {
			for (int j = 0; j < tinfo->N; j++) {
				double val = (dm2dGetEntry(matrix_copies[atual], i,   j + 1) +
				              dm2dGetEntry(matrix_copies[atual], i + 2, j + 1) +
				              dm2dGetEntry(matrix_copies[atual], i + 1, j) +
				              dm2dGetEntry(matrix_copies[atual], i + 1, j + 2)) / 4;
				// calcular delta
				double delta = fabs(val - dm2dGetEntry(matrix_copies[atual], i + 1, j + 1));
				if (delta > max_delta) {
					max_delta = delta;
				}
				dm2dSetEntry(matrix_copies[prox], i + 1, j + 1, val);
			}
		}
		// barreira de sincronizacao; calcular delta global
		global_delta = dualBarrierWait(dual_barrier, atual, max_delta);
	} while (++iter < tinfo->iter && global_delta >= tinfo->maxD);

	return EXIT_SUCCESS;
}

/*--------------------------------------------------------------------
| Function: main
| Description: Entrada do programa
---------------------------------------------------------------------*/
int main (int argc, char** argv) {
	double tEsq, tSup, tDir, tInf;
	int iter, trab, tam_fatia, res;

	if (argc != 11) {
		fprintf(stderr, "Utilizacao: ./heatSim N tEsq tSup tDir tInf iter trab maxD fichS periodoS\n\n");
		die("Numero de argumentos invalido");
	}

	// Ler Input
	N    = parse_integer_or_exit(argv[1], "N",    1);
	tEsq = parse_double_or_exit (argv[2], "tEsq", 0);
	tSup = parse_double_or_exit (argv[3], "tSup", 0);
	tDir = parse_double_or_exit (argv[4], "tDir", 0);
	tInf = parse_double_or_exit (argv[5], "tInf", 0);
	iter = parse_integer_or_exit(argv[6], "iter", 1);
	trab = parse_integer_or_exit(argv[7], "trab", 1);
	maxD = parse_double_or_exit (argv[8], "maxD", 0);
	fichS = argv[9];
	periodoS = parse_integer_or_exit(argv[10], "periodoS", 0);

	if (N % trab != 0) {
		fprintf(stderr, "\nErro: Argumento %s e %s invalidos.\n"
		        "%s deve ser multiplo de %s.", "N", "trab", "N", "trab");
		return EXIT_FAILURE;
	}

	//Inicializacao dos sinais
	sigset_t   signal_mask;

	if (sigemptyset (&signal_mask) == -1)
		die("Erro ao fazer sigemptyset");

	if (sigaddset (&signal_mask, SIGINT) == -1)
		die("Erro ao fazer sigaddset");

	if (sigaddset (&signal_mask, SIGALRM))
		die("Erro ao fazer sigaddset");

	if (pthread_sigmask (SIG_BLOCK, &signal_mask, NULL) == -1)
		die("Erro ao fazer sigmask");

	// Inicializar Barreira
	dual_barrier = dualBarrierInit(trab);

	//verificacao do ficheiro
	if (dual_barrier == NULL)
		die("Nao foi possivel inicializar barreira");

	aux_fich = (char* ) malloc (sizeof(char) * (sizeof(fichS) + 1));
	if (aux_fich == NULL)
		die("Erro ao alocar memoria para nome do ficheiro");

	strcpy(aux_fich, fichS);
	strcat(aux_fich, "~");

	if(aux_fich == NULL)
		die("Erro ao criar nome de ficheiro auxiliar");

	// Calcular tamanho de cada fatia
	tam_fatia = N / trab;

	FILE *f = fopen(fichS, "r");

	if (f !=  NULL) {
		matrix_copies[0] = readMatrix2dFromFile (f, N + 2, N + 2);
		fclose(f);
	}

	if (matrix_copies[0] == NULL) {
		if ((matrix_copies[0] = dm2dNew(N + 2, N + 2)) == NULL)
			die("Erro ao criar matriz");

		dm2dSetLineTo (matrix_copies[0], 0, tSup);
		dm2dSetLineTo (matrix_copies[0], N + 1, tInf);
		dm2dSetColumnTo (matrix_copies[0], 0, tEsq);
		dm2dSetColumnTo (matrix_copies[0], N + 1, tDir);
	}


	if ( (matrix_copies[1] = dm2dNew(N + 2, N + 2)) == NULL)
		die("Erro ao criar matriz");

	dm2dCopy (matrix_copies[1], matrix_copies[0]);

	// Reservar memoria para trabalhadoras
	thread_info *tinfo = (thread_info*) malloc(trab * sizeof(thread_info));
	pthread_t *trabalhadoras = (pthread_t*) malloc(trab * sizeof(pthread_t));

	if (tinfo == NULL || trabalhadoras == NULL) {
		die("Erro ao alocar memoria para trabalhadoras");
	}

	// Criar trabalhadoras
	for (int i = 0; i < trab; i++) {
		tinfo[i].id = i;
		tinfo[i].N = N;
		tinfo[i].iter = iter;
		tinfo[i].trab = trab;
		tinfo[i].tam_fatia = tam_fatia;
		tinfo[i].maxD = maxD;
		res = pthread_create(&trabalhadoras[i], NULL, tarefa_trabalhadora, &tinfo[i]);
		if (res != 0) {
			die("Erro ao criar uma tarefa trabalhadora");
		}
	}

	if (pthread_sigmask (SIG_UNBLOCK, &signal_mask, NULL) == -1)
		die("Erro ao fazer sigmask");

	struct sigaction actionAlarm;

	//so fazemos a salvaguarda caso periodoS != 0
	if (periodoS) {
		actionAlarm.sa_handler = trata_alarm;

		if (sigemptyset (&actionAlarm.sa_mask) == -1)
			die("Erro ao fazer sigemptyset");

		if (sigaction(SIGALRM, &actionAlarm, NULL) == -1)
			die("Erro ao fazer sigaction");
	}

	struct sigaction actionC;

	actionC.sa_handler = trataCtrlC_main;

	if (sigemptyset (&actionC.sa_mask ) == -1)
		die("Erro ao fazer sigemptyset");

	if (sigaction(SIGINT, &actionC, NULL) == -1)
		die("Erro ao fazer sigaction");

	alarm (periodoS);

	// Esperar que as trabalhadoras terminem
	for (int i = 0; i < trab; i++) {
		res = pthread_join(trabalhadoras[i], NULL);
		if (res != 0)
			die("Erro ao esperar por uma tarefa trabalhadora");
	}

	if (pthread_sigmask (SIG_BLOCK, &signal_mask, NULL) == -1)
		die("Erro ao fazer sigmask");

	if (should_I_continue) {
		if (unlink(fichS) == -1 && errno != ENOENT)
			die("Não foi possível apagar o ficheiro");
	}

	else {
		int wait_value = waitpid(pid_currently_saving, NULL, 0);

		if (wait_value == -1)
			die("Erro ao fazer waitpid");
	}

	dm2dPrint (matrix_copies[dual_barrier->iteracoes_concluidas % 2]);

	// Libertar memoria
	dm2dFree(matrix_copies[0]);
	dm2dFree(matrix_copies[1]);
	free(aux_fich);
	free(tinfo);
	free(trabalhadoras);
	dualBarrierFree(dual_barrier);

	return EXIT_SUCCESS;
}
