#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

char *leer_comandos() {
	int tamanio_buffer = sizeof(char) *20;
	char *buffer = (char *)malloc(tamanio_buffer);
       	
	if(buffer == NULL) {
		printf("Error de memoria");
		exit(EXIT_FAILURE);
	}

	int i = 0;
	char caracter_actual = getchar();
	char *aux;

	while(caracter_actual != '\n') {
		if(i >= (tamanio_buffer - 1)) {
			tamanio_buffer *= 2;
			aux = (char *)realloc(buffer, tamanio_buffer);
			if(aux == NULL) {
				printf("Error de memoria por realloc\n");
				free(buffer);
				exit(EXIT_FAILURE);
			}
		      	buffer = aux;	
		}
		buffer[i] = (char) caracter_actual;
		i++;
		caracter_actual = getchar();
	}	
	buffer[i] = '\0';
	
	return buffer;
}

int main(void) {
	
	printf("Nombre y formato de comandos: \n");
	printf("tokenizar <nombre_archivo>\n");

	printf("analizar <nombre_archivo>\n");
	printf("finalizar\n");
	printf("-------------\n");


	char *linea;
	char *comando;
	char *nombre_archivo;
	char *chequeo_parametros;

	pid_t childPid;

	while(1){
		
		printf("Ingrese comando: \n");
		linea = leer_comandos();
		
		if(strcmp(linea, "finalizar") == 0) {
			printf("Saliendo \n");
			free(linea);
			break;
		}
		
		comando = strtok(linea, " \t");
		nombre_archivo = strtok(NULL, " \t");
		chequeo_parametros = strtok(NULL, " \t");

		if((comando != NULL) && (nombre_archivo != NULL)) {
			if(chequeo_parametros != NULL) {
				printf("Cantidad de argumentos mayor a 2, no se puede procesar \n");
			} else {
				if((strcmp(comando, "tokenizar")) == 0 || (strcmp(comando, "vectorizar")) == 0 || (strcmp(comando, "analizar")) == 0) {
					childPid = fork();
					if(childPid == 0){
						char path[20];
						sprintf(path, "./%s", comando);
						execl(path, comando, nombre_archivo, (char *) NULL);
					}

				} else printf("Error de sintaxis, comando: %s \n", comando);
			}

		} else {
			printf("No se ingresó un comando y/o archivo \n"); 
		}
		


	}

	exit(EXIT_SUCCESS);
}
