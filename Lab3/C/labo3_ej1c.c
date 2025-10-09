#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>


void *calcular_sumatoria(void *arg){
	int *n = (int *) arg;
	int i;
	int sum = 0;
	for(i = 0; i < *n ; i++) {
		sum += i;
	}
	*n = sum;	
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
	int sumatorias[cantHilos];

	for(i = 0; i < cantHilos; i++){
		sumatorias[i] = i + cantHilos;
		retornoCreate =	pthread_create(&hilos[i], NULL, calcular_sumatoria, (void *) &sumatorias[i]);
		if(retornoCreate != 0) {
			printf("Error creando hilo\n");
			exit(1);
		}
	}

	for(i = 0; i < cantHilos; i++){
		pthread_join(hilos[i], NULL);
		printf("Termino hilo %d con sumatoria(de %d a %d): %d\n", (i+1), 0, ((i + cantHilos) - 1), sumatorias[i]);
	}

	return 0;
}
