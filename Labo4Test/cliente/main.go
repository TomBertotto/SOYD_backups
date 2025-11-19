package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"strconv"
	"os"
)


const BLOCK_TAMANIO = 1024

func generarID(nombre_archivo string, bloque int) string {
	base := strings.TrimSuffix(nombre_archivo, ".txt")
	return fmt.Sprintf("%s_b%d.txt", base, bloque)
}

func dividirEnBloques(archivo []byte) [][]byte {
	var bloques[][]byte
	for i:= 0; i < len(archivo); i+=BLOCK_TAMANIO {
		fin := i + BLOCK_TAMANIO
		if fin > len(archivo) {
			fin = len(archivo)
		}
		bloques = append(bloques, archivo[i:fin])
	}
	return bloques
}

func enviarBloqueADatanode(addr string, blockID string, data[]byte) error {
	
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("CLIENTE: error al conectar a DATANODE:", err)
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "store %s\n", blockID)
	_, err = conn.Write(data)
	if err != nil {
		fmt.Println("CLIENTE: error enviando bloque:", err)
		return err
	}
	return nil
}

func ejecutarPut(nombre_archivo string, addrNamenode string) {
	archivo, err := os.ReadFile(nombre_archivo)
	if err != nil {
		fmt.Println("Error abriendo archivo:", err)
		return
	}
	
	bloques := dividirEnBloques(archivo)
	cant_bloques := len(bloques)
	
	fmt.Printf("Archivo: %s (%d bytes) -> %d bloques de 1KB\n", nombre_archivo, len(archivo), cant_bloques)

	msg := fmt.Sprintf("put %s %d\n", nombre_archivo, cant_bloques)



	conn, err := net.Dial("tcp", addrNamenode) //me conecto al namenode
	if err != nil {
		fmt.Println("Error conectado con el namenode:", err)
		return
	}
	defer conn.Close()

	conn.Write([]byte(msg))
	
	fmt.Println("Cliente esperando asignacion de datanodes...")
	
	reader := bufio.NewReader(conn)
	asignaciones_bloques := make(map[int]string)

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
		partes := strings.Fields(linea)
		if len(partes) == 2 {
			blockIDStr := strings.TrimPrefix(partes[0], "b")
			blockID, err := strconv.Atoi(blockIDStr)
			if err == nil {
				asignaciones_bloques[blockID] = partes[1]
			}
		}
	}

	fmt.Println("CLIENTE: enviando datos a DATANODES")

	for bloqueID, addrDatanode := range asignaciones_bloques {
		data := bloques[bloqueID]
		fmt.Printf("Enviando bloque %d a %s\n", bloqueID, addrDatanode)
		id_bloque := generarID(nombre_archivo, bloqueID)//CONVENCION: tomo que el id es nombre_archivo_b0.txt, nombre_archivo_b1.txt...
		err := enviarBloqueADatanode(addrDatanode, id_bloque, data) //me conecto a los datanodes
		if err != nil {
			fmt.Printf("CLIENTE: error enviando el bloque %d -> %s\n", bloqueID, addrDatanode)
		}
	}
	
	fmt.Println("CLIENTE: se completo la transferencia")
	fmt.Println("CLIENTE: enviando ACK al NAMENODE")
	fmt.Fprintf(conn, "ACK\n")
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


