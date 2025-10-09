#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

static const char *POSITIVAS[] = {"felicidad", "contento", "libre", "emotivo", "mejor"};
static const char *NEGATIVAS[] = {"peor", "tristeza", "injusto", "odio", "bronca"};

char *leer_linea(FILE* archivo) {
	int tamanio_buffer = sizeof(char) *20;
	char *buffer = (char *)malloc(tamanio_buffer);
       	
	if(buffer == NULL) {
		printf("Error de memoria");
		exit(EXIT_FAILURE);
	}

	int i = 0;
	char caracter_actual = tolower((unsigned char)fgetc(archivo));
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
		caracter_actual = tolower((unsigned char)fgetc(archivo));
	}	
	buffer[i++] = '\n';
	buffer[i] = '\0';	
	return buffer;
}


char *sacar_formato(char *archivo){
	char *nuevo_nombre = (char *)malloc(strlen(archivo));
	int i = 0;
	while ((i < strlen(archivo)) && (archivo[i] != '.')){
		nuevo_nombre[i] = archivo[i];
		i++;
	}
	return nuevo_nombre;
}


int main(int argc, char *argv[]){

	char *nombre_archivo_entrada = argv[1];
	FILE* archivo_entrada = fopen(nombre_archivo_entrada, "r");
	char *delimitadores = " \t\n.,;:!?¿()[]{}\"'";

	if(archivo_entrada == NULL) {
		printf("Error al leer el archivo\n");
		exit(EXIT_FAILURE);
	}

	char formato[] = "_analisis.txt";
	char *nombre_sin_formato = sacar_formato(nombre_archivo_entrada);
	char archivo_output[strlen(nombre_sin_formato) + strlen(formato) + 1];
	strcpy(archivo_output, nombre_sin_formato);
	strcat(archivo_output, formato);	
	FILE* archivo_analizado = fopen(archivo_output, "w");

	char *linea;
	char *token_actual;
	int cont_positivas = 0;
	int cont_negativas = 0;
	

	while((linea = leer_linea(archivo_entrada)) != NULL){
		token_actual = strtok(linea, delimitadores);
		while(token_actual != NULL) {
			if((strcmp(token_actual, POSITIVAS[0] ) == 0) || (strcmp(token_actual,POSITIVAS[1] ) == 0)
				|| (strcmp(token_actual, POSITIVAS[2]) == 0) || (strcmp(token_actual,POSITIVAS[3]) == 0)
			       		|| (strcmp(token_actual, POSITIVAS[4]) == 0))
				cont_positivas++;

			if((strcmp(token_actual, NEGATIVAS[0]) == 0) || (strcmp(token_actual, NEGATIVAS[1]) == 0)
				|| (strcmp(token_actual, NEGATIVAS[2]) == 0) || (strcmp(token_actual, NEGATIVAS[3]) == 0)
			       		|| (strcmp(token_actual, NEGATIVAS[4]) == 0))
				cont_negativas++;

			token_actual = strtok(NULL, delimitadores);
		}
		free(linea);
	}

	fprintf(archivo_analizado, "El resultado es: ");
	if(cont_positivas > cont_negativas) {
		fprintf(archivo_analizado, "POSITIVO. ---- %d palabras positivas\n", cont_positivas);
	} else if(cont_positivas < cont_negativas) {
		fprintf(archivo_analizado, "NEGATIVO. ---- %d palabras negativas\n", cont_negativas);
	} else fprintf(archivo_analizado, "NEUTRO. ---- %d palabras positivas | %d palabras negativas\n", cont_positivas, cont_negativas);

	fclose(archivo_entrada);
	fclose(archivo_analizado);
	free(linea);
	free(nombre_sin_formato);
}

