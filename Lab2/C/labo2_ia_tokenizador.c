#include <stdio.h>
#include <string.h>
#include <stdlib.h>

char *leer_linea(FILE* archivo) {
	int tamanio_buffer = sizeof(char) *20;
	char *buffer = (char *)malloc(tamanio_buffer);
       	
	if(buffer == NULL) {
		printf("Error de memoria");
		exit(EXIT_FAILURE);
	}

	int i = 0;
	char caracter_actual = fgetc(archivo);
	char *aux;
	if(caracter_actual == EOF) {
		free(buffer);
		return NULL;
	}

	while(caracter_actual != '\n' && caracter_actual != EOF) {
		if(i >= (tamanio_buffer - 2)) { //para luego agregar enter y terminador nulo
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
		caracter_actual = fgetc(archivo);
	}	
	buffer[i++] = '\n';
	buffer[i] = '\0';	
	return buffer;
}


int main(int argc, char *argv[]){


	char *nombre_archivo_entrada = argv[1];
	FILE* archivo_entrada = fopen(nombre_archivo_entrada, "r");
	char *delimitadores = " \t\n.,;:!?¿()[]{}\"'";

	if(archivo_entrada == NULL) {
		printf("Error al leer el archivo\n");
		exit(EXIT_FAILURE);
	}

	char formato[] = "_tokens.txt";
	char archivo_output[strlen(nombre_archivo_entrada) + strlen(formato) + 1];
	strcpy(archivo_output, nombre_archivo_entrada);
	strcat(archivo_output, formato);
	FILE* archivo_tokenizado = fopen(archivo_output, "w");


	fprintf(archivo_tokenizado, "[");
	
	int primera_palabra = 1; // para evitar agregar una coma al comienzo
	char *token_actual;
	char *linea;

	while((linea = leer_linea(archivo_entrada)) != NULL) {
		token_actual = strtok(linea, delimitadores);
		while(token_actual != NULL) {

			if(primera_palabra == 1) {
				primera_palabra = 0;
			} else {
				fprintf(archivo_tokenizado, ", ");
			}
			fprintf(archivo_tokenizado,"%s", token_actual);
			token_actual = strtok(NULL, delimitadores);
		}
		free(linea);
	}

	fprintf(archivo_tokenizado, "]\n");
	fclose(archivo_entrada);
	fclose(archivo_tokenizado);
	exit(EXIT_SUCCESS);
}
