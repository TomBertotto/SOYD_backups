package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"os"
)

const (
	servidor_URL = "https://desarrollo.cs.uns.edu.ar"
	formato_archivo = "labo2_ia_vector_"
	//pag_principal_url = servidor_URL + "/~user_00/index-2025.php"

)

/*
func obtenerUsuarios() ([]string, error) {
	request_al_index, err := http.Get(pag_principal_url)
	if err != nil {
		fmt.Errorf("Error en request al index")
		return nil
	}
	defer request_al_index.Body.Close()

	cuerpo, err := io.ReadAll(request_al_index.Body)
	if err != nil {
		fmt.errorf("Error al leer el cuerpo")
		return nil
	}

	expresion_regular := regexp.MustCompile(`alu_\d+`)
	coinciden := expresion_regular.FindAllString(string(cuerpo), -1)
	return coinciden, nil
} */

func descargar_analizar(usuario string, wg *sync.WaitGroup, palabras []string) {
	defer wg.Done()
	url := fmt.Sprintf("%s/~%s/%s%s", servidor_URL, usuario, formato_archivo, usuario + ".txt")
	
	request, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error en usuario: %s, error %v\n", usuario, err)
		return
	}
	defer request.Body.Close()

	if request.StatusCode == http.StatusNotFound {
		fmt.Printf("Archivo no encontrado del usuario: %s\n", usuario)
		return
	}
	
	contenido, err := io.ReadAll(request.Body) //leo en lugar de descargar el archivo entero
	if err != nil {
		fmt.Printf("Error al leer el archivo del usuario %s, error: %v\n", usuario, err)
		return
	}
	
	analizarArchivo(usuario, string(contenido), palabras)
}

func analizarArchivo(usuario, contenido string, palabras []string) {
	lineas := strings.Split(strings.TrimSpace(contenido), "\n")
	if len(lineas) < 2 {
		fmt.Printf("Mal formato de texto, no contiene 2 lineas")
		return
	}
	linea_palabras := strings.Trim(lineas[0], "[] ")
	linea_valores := strings.Trim(lineas[1], "[] ")

	palabras_archivo := strings.Split(linea_palabras, ",")
	valores_archivo := strings.Split(linea_valores, ",")
	if len(palabras_archivo) != len(valores_archivo) {
		fmt.Println("No coinciden la cantidad de palabras con cantidad de valores\n")
		os.Exit(1)
	}
	for i:= 0; i < len(palabras_archivo); i++ {
		palabras_archivo[i] = strings.TrimSpace(palabras_archivo[i])
		valores_archivo[i] = strings.TrimSpace(valores_archivo[i])
	}

	fmt.Printf("Palabras %s \nValores: %s\n", palabras_archivo, valores_archivo)
}

func main() {
	
	if len(os.Args) < 4 {
		fmt.Println("Error en cantidad de argumentos")
		fmt.Println("Formato: ./labo3_ej3 <palabra1> <palabra2> <palabra3>")
		os.Exit(1)
	}

	var palabras_analizar []string
	palabras_analizar = make([]string, len(os.Args) - 1)
	for i := 0; i  < len(palabras_analizar); i++ {
		palabras_analizar[i] = os.Args[i + 1]
	}


	cant_alumnos := 1
	var lu_alumnos[1]string
	lu_alumnos[0] = "alu_145294"
	var wg sync.WaitGroup
	for i := 0; i < cant_alumnos ; i++ {
		wg.Add(1)
		go descargar_analizar(lu_alumnos[i], &wg, palabras_analizar)
	}
	wg.Wait()
	
}	
