#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>



void *mostrar_hilo(void *arg){
	pthread_t hilo_id = pthread_self();
	printf("Hilo con identificador: %d\n", (int) hilo_id);
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
		retornoCreate =	pthread_create(&hilos[i], NULL, mostrar_hilo, NULL);
		if(retornoCreate != 0) {
			printf("Error creando hilo\n");
			exit(1);
		}
	}

	for(i = 0; i < cantHilos; i++){
		pthread_join(hilos[i], NULL);
	}

	return 0;
}
