package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"os"
	"strconv"
	"regexp"
)

const (
	servidor_URL = "https://desarrollo.cs.uns.edu.ar"
	formato_archivo = "labo2_ia_vector_"
	pag_principal_url = servidor_URL + "/~user_00/index-2025.php"	
	tamanio_arreglos = 5
)


func obtenerUsuarios() ([]string, error) {
	request_al_index, err := http.Get(pag_principal_url)
	if err != nil {
		fmt.Println("ERROR AL LEER LOS USUARIOS\n")
		return nil, err
	}
	defer request_al_index.Body.Close()

	cuerpo, err := io.ReadAll(request_al_index.Body)
	if err != nil {
		fmt.Println("ERROR AL LEER USUARIOS INDEX")
		return nil, err
	}

	expresion_regular := regexp.MustCompile(`alu_\d+`)
	coinciden := expresion_regular.FindAllString(string(cuerpo), -1)
	return coinciden, nil
}


func analizarArchivo(usuario, contenido string, palabras []string) []int  {
	lineas := strings.Split(strings.TrimSpace(contenido), "\n")
	if len(lineas) < 2 {
		fmt.Printf("Mal formato de texto del usuario %s, no contiene 2 lineas\n", usuario)
		return nil
	}
	linea_palabras := strings.Trim(lineas[0], "[] ")
	linea_valores := strings.Trim(lineas[1], "[] ")

	palabras_archivo := strings.Split(linea_palabras, ",")
	valores_archivo := strings.Split(linea_valores, ",")
	if len(palabras_archivo) != len(valores_archivo) {
		fmt.Printf("No coinciden la cantidad de palabras con cantidad de valores en el archivo del usuario: %s\n", usuario)
		return nil
	}
	
	pesos := make([]int, len(palabras))

	for i:= 0; i < len(palabras_archivo); i++ {
		palabras_archivo[i] = strings.TrimSpace(palabras_archivo[i])
		palabras_archivo[i] = strings.ToLower(palabras_archivo[i])
		valores_archivo[i] = strings.TrimSpace(valores_archivo[i])
		valores_archivo[i] = strings.ToLower(valores_archivo[i])
		for j:= 0; j < len(palabras); j++ {
			conversion, err := strconv.Atoi(valores_archivo[i])
			if err == nil {
				if palabras_archivo[i] == palabras[j] {
					pesos[j] = conversion
				}
			}
		}
	}
	return pesos
}



func main() {
	
	if len(os.Args) < 4 {
		fmt.Println("Error en cantidad de argumentos")
		fmt.Println("Formato: ./labo3_ej3 <palabra1> <palabra2> <palabra3>")
		fmt.Println("")	
		fmt.Println("Palabras positivas: bueno, genial, positivo, feliz, contento")
		fmt.Println("Palabras negativas: peor, triste, odio, terrible, negativo")
		fmt.Println("Palabras testeo: probar, prueba, test, intento, funciona")
		os.Exit(1)
	}
	
	p_positivas := [tamanio_arreglos]string{"bueno", "correcto", "positivo", "feliz", "contento"}
	p_negativas := [tamanio_arreglos]string{"peor", "triste", "odio", "mal", "falla"}
	p_testeo := [tamanio_arreglos]string{"prueba", "correcto", "test", "falla", "funciona"}


	usuarios, err := obtenerUsuarios()
	if err != nil {
		fmt.Println("ERROR en obtener los usuarios\n")
		os.Exit(1)
	}
	type pesoUsuarios struct {
		usuario string
		pesos []int
	}

	resultados := make([]pesoUsuarios, len(usuarios))
	var palabras_analizar []string
	palabras_analizar = make([]string, len(os.Args) - 1)
	var palabras_coinciden []string
	palabras_coinciden = make([]string, len(os.Args) - 1)
	
	contP := 0
	contN := 0
	contT := 0

	for i := 0; i  < len(palabras_analizar); i++ {
		palabras_analizar[i] = os.Args[i + 1]
		for j := 0; j < tamanio_arreglos; j++ {
			if palabras_analizar[i] == p_positivas[j] {contP = 1}	
			if palabras_analizar[i] == p_negativas[j] {contN = 1}
			if palabras_analizar[i] == p_testeo[j] {contT = 1}
		}
		fmt.Printf("Palabra '%s' con categoría: ", palabras_analizar[i])
		if contT != 0 { fmt.Printf("testeo ")}
		if contP != 0 { fmt.Printf("positivo")}
		if contN != 0 { fmt.Printf("negativo")}
		if contT == 0 && contP == 0 && contN == 0 {
			fmt.Println("[NO ENCONTRADA EN LISTA]")
		} else {
			palabras_coinciden = append(palabras_coinciden, palabras_analizar[i])	
		}
		contP = 0
		contN = 0
		contT = 0
		fmt.Println("")
	}
	fmt.Println("======================================\n")

	var wg sync.WaitGroup
	for i, usuario := range usuarios {
		wg.Add(1)
		go func(index int, us string) {
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
				fmt.Printf("-----------------------------------\n")
				return
			}
	
			contenido, err := io.ReadAll(request.Body) //leo en lugar de descargar el archivo entero
			if err != nil {
				fmt.Printf("Error al leer el archivo del usuario %s, error: %v\n", usuario, err)
				return
			}

			resultado_actual := analizarArchivo(us, string(contenido), palabras_coinciden)
			resultados[index] = pesoUsuarios{usuario: us, pesos: resultado_actual}
		}(i, usuario)

	}
	
	wg.Wait()
	for _, res := range resultados {
		if res.pesos == nil {
			continue
		}
		fmt.Printf("Resultado del archivo del usuario %s\n", res.usuario)
		for i, palabra := range palabras_coinciden {
			fmt.Printf("%s: %d\n", palabra, res.pesos[i])
		}
		fmt.Println("------------------------------------")
	}
}

