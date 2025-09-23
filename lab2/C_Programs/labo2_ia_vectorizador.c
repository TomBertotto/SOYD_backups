#include <stdio.h>
#include <string.h>
#include <stdlib.h>

typedef struct {
	char *palabra;
	int frecuencia;

} ParFrecuencia;


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

	char formato[] = "_vector.txt";
	char archivo_output[strlen(nombre_archivo_entrada) + strlen(formato) + 1];
	strcpy(archivo_output, nombre_archivo_entrada);
	strcat(archivo_output, formato);
	FILE* archivo_vectorizado = fopen(archivo_output, "w");

	ParFrecuencia *tabla_pares = NULL;
	int tamanio_tabla = 0;
	char * token_actual;
	int i;
	int encontre_inserte = 0;
	char *linea;

	while((linea = leer_linea(archivo_entrada)) != NULL){
		token_actual = strtok(linea, delimitadores);
		while(token_actual != NULL) { //para ver todos los tokens que guarde en el buffer
			i=0;
			encontre_inserte = 0;	
			while((i < tamanio_tabla) && (!encontre_inserte)) {
				if(strcmp(tabla_pares[i].palabra, token_actual) == 0) {
					tabla_pares[i].frecuencia++;
					encontre_inserte = 1;
				}
				i++;
			}	
			if(!encontre_inserte) {
				tabla_pares = (ParFrecuencia *)realloc(tabla_pares, (tamanio_tabla + 1) * sizeof(ParFrecuencia));
				tabla_pares[tamanio_tabla].palabra = strdup(token_actual);
				tabla_pares[tamanio_tabla].frecuencia = 1;
				tamanio_tabla++;
			}	
			token_actual = strtok(NULL, delimitadores);
		}
		free(linea);
	}


	//bubble sort para ordenar los pares de mayor frecuencia a menor frecuencia
	ParFrecuencia aux;

	for(int j=0; j < tamanio_tabla - 1; j++){
		for(int k=0; k < tamanio_tabla -j - 1; k++){
			if(tabla_pares[k].frecuencia < tabla_pares[k+1].frecuencia){
				aux = tabla_pares[k+1];
				tabla_pares[k+1] = tabla_pares[k];
				tabla_pares[k] = aux;		
			}
		}
	}


	fprintf(archivo_vectorizado, "Vocabulario: [");
	int primera_palabra = 1;
	for(int j=0; j < tamanio_tabla; j++){
		if(primera_palabra) {
			primera_palabra = 0;
		} else fprintf(archivo_vectorizado,", ");

		fprintf(archivo_vectorizado, "%s", tabla_pares[j].palabra);
	}

	fprintf(archivo_vectorizado, "]\n");
	fprintf(archivo_vectorizado, "Vector: [");
	
	primera_palabra = 1;
	for(int j=0; j < tamanio_tabla; j++){
		if(primera_palabra) {
			primera_palabra = 0;
		} else fprintf(archivo_vectorizado,", ");

		fprintf(archivo_vectorizado, "%d", tabla_pares[j].frecuencia);
	}

	fprintf(archivo_vectorizado, "]\n");


	for(int k=0; k < tamanio_tabla; k++){ //necesario por usar strdup antes
		free(tabla_pares[k].palabra);
	}

	free(tabla_pares);
	tabla_pares = NULL;
	fclose(archivo_entrada);
	fclose(archivo_vectorizado);
	exit(EXIT_SUCCESS);

}
