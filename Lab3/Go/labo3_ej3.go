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


type pesoUsuarios struct {
	usuario string
	pesos []int
	resultado int
	es_test bool
}

//-------------PALABRAS DE LA BD------------------

var p_positivas = [tamanio_arreglos]string{"bueno", "correcto", "positivo", "feliz", "contento"}
var p_negativas = [tamanio_arreglos]string{"peor", "triste", "odio", "mal", "falla"}
var p_testeo = [tamanio_arreglos]string{"prueba", "correcto", "test", "falla", "funciona"}
//------------------------------------------------


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


func analizarArchivo(usuario, contenido string, palabras []string, pesoUser *pesoUsuarios)  {
	lineas := strings.Split(strings.TrimSpace(contenido), "\n")
	if len(lineas) < 2 {
		fmt.Printf("Mal formato de texto del usuario %s, no contiene 2 lineas\n", usuario)
		return
	}
	linea_palabras := strings.Trim(lineas[0], "[] ")
	linea_valores := strings.Trim(lineas[1], "[] ")

	palabras_archivo := strings.Split(linea_palabras, ",")
	valores_archivo := strings.Split(linea_valores, ",")
	if len(palabras_archivo) != len(valores_archivo) {
		fmt.Printf("No coinciden la cantidad de palabras con cantidad de valores en el archivo del usuario: %s\n", usuario)
		return
	}
	
	pesoUser.usuario = usuario
	pesoUser.pesos = make([]int, len(palabras))
	pesoUser.es_test = false

	for i:= 0; i < len(palabras_archivo); i++ {
		palabras_archivo[i] = strings.TrimSpace(palabras_archivo[i])
		palabras_archivo[i] = strings.ToLower(palabras_archivo[i])
		valores_archivo[i] = strings.TrimSpace(valores_archivo[i])
		valores_archivo[i] = strings.ToLower(valores_archivo[i])
		for j:= 0; j < len(palabras); j++ {
			conversion, err := strconv.Atoi(valores_archivo[i])
			if err == nil {
				if palabras_archivo[i] == palabras[j] {
					pesoUser.pesos[j] = conversion
					
					if !pesoUser.es_test {
						pesoUser.es_test = esPalabraTest(palabras_archivo[i])
					}

					if esPalabraPositiva(palabras[j]) {
						pesoUser.resultado += conversion
					}

					if esPalabraNegativa(palabras[j]) {
						pesoUser.resultado -= conversion
					}
				}
			}
		}
	}
	
}


func esPalabraNegativa(palabra string) bool {
	var es = false
	for i:= 0; i < tamanio_arreglos; i++ {
		if p_negativas[i] == palabra {
			es = true
			break
		}
	}
	return es
}

func esPalabraPositiva(palabra string) bool {
	var es = false
	for i:= 0; i < tamanio_arreglos; i++ {
		if p_positivas[i] == palabra {
			es = true
			break
		}
	}
	return es
}

func esPalabraTest(palabra string) bool {
	var es_test = false
	for i:= 0; i < tamanio_arreglos; i++ {
		if p_testeo[i] == palabra {
			es_test = true
			break
		}
	}
	return es_test
}

func filtrarUsuarios(usuarios []string) []string {
	var filtrados[]string
	var duplicado  bool = false
	for _, actual := range usuarios {
		duplicado = false
		for _, filtrado := range filtrados {
			if filtrado == actual {
				duplicado = true
				break
			}
		}
		if !duplicado {
			filtrados = append(filtrados, actual)
		}
	}
	return filtrados
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


	usuarios, err := obtenerUsuarios()
	if err != nil {
		fmt.Println("ERROR en obtener los usuarios\n")
		os.Exit(1)
	}
	
	usuarios = filtrarUsuarios(usuarios) //algunos quedan duplicados y los saco manualmente
	
	resultados := make([]pesoUsuarios, len(usuarios))
	var palabras_analizar []string
	palabras_analizar = make([]string, len(os.Args) - 1)
	var palabras_coinciden []string
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
	

	fmt.Println("=======================================================")
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

			if request.StatusCode == http.StatusNotFound || request.StatusCode != http.StatusOK {
				return
			}

			archivo_nuevo, err := os.Create(formato_archivo + usuario + ".txt")
			if err != nil {
				fmt.Printf("No se pudo crear el archivo\n")
				return
			}

			defer archivo_nuevo.Close()

			_, err = io.Copy(archivo_nuevo, request.Body)
			if err != nil {
				fmt.Printf("Error al copiar al archivo de salida\n")
				return
			}
			
			_, err = archivo_nuevo.Seek(0, 0)
			if err != nil {
				fmt.Printf("Error al reposicionar puntero al comienzo del archivo\n")
				return
			}
			
			contenido, err := io.ReadAll(archivo_nuevo)
			if err != nil {
				fmt.Printf("Error al leer del archivo creado\n")
				return
			}
		
			analizarArchivo(us, string(contenido), palabras_coinciden, &resultados[index])
		
		}(i, usuario)

	}

	wg.Wait()
	fmt.Println("=======================================================")
	
	for _, res := range resultados {
		if res.pesos == nil {
			continue
		}
		fmt.Printf("Resultado (<palabra>: <peso>) del archivo del usuario %s\n", res.usuario)
		fmt.Println("")
		for i, palabra := range palabras_coinciden {
			fmt.Printf(" '%s': %d |", palabra, res.pesos[i])
		}
		fmt.Println()
		fmt.Printf("Resultado final: ")
		if res.es_test {
			fmt.Printf("TESTEO ")
		}

		if res.resultado > 0 {
			fmt.Printf("POSITIVO\n")
		} else if res.resultado < 0 {
			fmt.Printf("NEGATIVO\n")
		} else {
			fmt.Printf("NEUTRO\n")
		}
		fmt.Println("")
		fmt.Println("-----------------------------------------------------------------------")
		fmt.Println("")
	}
}

