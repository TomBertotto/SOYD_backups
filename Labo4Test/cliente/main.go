package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"os"
)


const BLOCK_TAMANIO = 1024

func ejecutarPut(nombre_archivo string, addrNamenode string) {
	archivo, err := os.ReadFile(nombre_archivo)
	if err != nil {
		fmt.Println("Error abriendo archivo:", err)
		return
	}

	tamanio_archivo := len(archivo)
	cant_bloques := tamanio_archivo / BLOCK_TAMANIO
	if tamanio_archivo % BLOCK_TAMANIO != 0 {
		cant_bloques++
	}
	
	fmt.Printf("Archivo: %s (%d bytes) -> %d bloques de 1KB\n", nombre_archivo, tamanio_archivo, cant_bloques)

	msg := fmt.Sprintf("put %s %d\n", nombre_archivo, cant_bloques)

	conn, err := net.Dial("tcp", addrNamenode)
	if err != nil {
		fmt.Println("Error conectado con el namenode:", err)
		return
	}
	defer conn.Close()

	conn.Write([]byte(msg))
	
	fmt.Println("Cliente esperando asignacion de datanodes...")

	reader := bufio.NewReader(conn)
	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error al recibir del namenode")
			break
		}
		linea = strings.TrimSpace(linea)

		if linea == "END" { //establezco como protocolo que el namenode va a usar END al final
			break
		}

		fmt.Println("---", linea, "---")
	}

	//FALTA IMPLEMENTAR ENVIO A LOS DATANODES!!!!!!!!!!!!!!!!!!!!!!!


}



func procesarComando(input string, addrNamenode string) {
	partes := strings.Fields(input)
	comando := strings.ToLower(partes[0])

	switch comando {
	case "put": 
		if len(partes) < 2 { //ver si se puede refactorizar para no tener tanto codigo de conexion duplicado
			fmt.Println("Incorrecto, uso: put <archivo>")
			return
		}

		ejecutarPut(partes[1], addrNamenode)
	
	case "get":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: get <archivo>")
			return
		}
		//ejecutarGet(partes[1], addrNamenode)	
	
	case "ls":
		//ejecutarLsInfo(partes[0], addrNamenode)
	case "info":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: info <archivo>")
			return
		}
		//ejecutarLsInfo(partes[1], addrNamenode)
		
	default: fmt.Println("Comando no válido:", comando)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Parametros incorrectos")
		fmt.Println("Formato: go run client.go <IP_namenode>:<puerto>")
		return
	}

	addrNamenode := os.Args[1]
	reader:= bufio.NewReader(os.Stdin)

	fmt.Println("----------CLIENTE DFS----------")
	fmt.Println("Comandos: put <archivo>, get<archivo>, ls, info <archivo>, exit")

	for {
		fmt.Print("dfs> ")
		
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error leyendo la entrada:", err)
			continue
		}
		
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		if input == "exit" {
			fmt.Println("Saliendo del cliente...")
			return
		}

		procesarComando(input, addrNamenode)
	}
}


