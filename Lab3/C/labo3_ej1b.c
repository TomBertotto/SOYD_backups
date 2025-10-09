#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

/* IMPORTANTE:
 * asumo que los resultados
 * mostrados por pantalla no se piden en orden secuencial */

void *mostrar_sumatoria(void *arg){
	int *n = (int *) arg;
	int i;
	int sum = 0;
	for(i = 0; i < *n ; i++) {
		sum += i;
	}	
	printf("Numero %d con sumatoria (de %d a %d): %d\n", *n, 0, (*n - 1), sum);
	printf("-----------------\n");
	free(n);
	pthread_exit(0);
}

int main(int argc, char *argv[]){

	if(argc < 2) {
		printf("Error en cantidad de argumentos\n");
		exit(1);
	}

	int cantHilos = atoi(argv[1]);
	pthread_t hilos[cantHilos];
	int i;
	int retornoCreate;

	for(i = 0; i < cantHilos; i++){
		int *k = (int *)malloc(sizeof(int)); //para no pasar el mismo k a todos los hilos
		*k = i + cantHilos;
		retornoCreate =	pthread_create(&hilos[i], NULL, mostrar_sumatoria, (void *) k);
		if(retornoCreate != 0) {
			printf("Error creando hilo\n");
			free(k);
			exit(1);
		}
	}

	for(i = 0; i < cantHilos; i++){
		pthread_join(hilos[i], NULL);
	}

	return 0;
}
