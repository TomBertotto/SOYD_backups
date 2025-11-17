package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

const NAMENODE_ADDR = "localhost:8000"
const BLOCK_SIZE = 1024


type Asignacion struct {
	block string
	node string
}

func conectar(addr strings) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

func leerLinea(reader *bufio.Reader) (string, error) {
	linea, err :0 reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(linea), nil
}

func comandoLS() {
	conn, err := conectar(NAMENODE_ADDR)
	if err != nil {
		log.Println("Error conectandose al NAMENODE\n")
		return
	}
	defer conn.Close()

	fmt.Fprintf(conn, "LS\n")

	reader := bufio.NewReader(conn)
	linea, _ := leerLinea(reader)

	if !strings.HasPrefix(linea, "OK") {
		fmt.Println(linea)
		return
	}

	fmt.Println("Archivos en DFS:")
	for {
		archivo, err := leerLinea(reader)
		if err != nil {
			break
		}
		fmt.Println(" -", archivo)
	}
}

func comandoINFO(nombre string) {
	conn, err := conectar(NAMENODE_ADDR)
	if err != nil {
		log.Println("Error conectandose al NAMENODE\n")
		return
	}
	defer conn.Close()
	fmt.Fprintf(conn, "INFO%s\n", nombre)

	reader := bufio.NewReader(conn)
	linea, _ := leerLinea(reader)

	if !string.hasPrefix(linea, "OK") {
		fmt.Println(linea)
		return
	}

	fmt.Println("Bloques del archivo:")
	for {
		bloques, err := leerLinea(reader)
		if err != nil {
			break
		}
		fmt.Println(" ", bloques)
	}
}

func comandoPUT(path string) {
	archivo, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error leyendo archivo:", err)
		return
	}

	nombre_archivo := path
	total := len(archivo)
	numBlocks := (total + BLOCK_SIZE - 1) / BLOCK_SIZE

	conn, err := conectar(NAMENODE_ADDR)
	if err != nil {
		fmt.Println("No se pudo conectar al NAMENODE")
		return
	}

	defer conn.Close()

	fmt.Fprintf(conn, "PUT %s %d\n", nombre_archivo, numBlocks)

	reader := bufio.NewReader(conn)

	linea, err := leerLinea(reader)
	if err != nill | !strings.HasPrefix(linea, "OK") {
		fmt.Println("ERROR en PUT\n")
		return
	}

	asignaciones := make([]Asignacion, 0)

	for i:=0; i < numBlocks; i++ {
		l, _ := leerLinea(reader)
		partes := strings.Fields(l)
		asignaciones = append(asignaciones, Asignacion{partes[0], partes[1]})
	}

	offset := 0 

	for _, asignacion := range asignaciones {
		end := offset + BLOCKSIZE
		if end > total {
			end = total
		}

		data := archivo[offset:end]
		offset += BLOCK_SIZE

		direcNodo := asignacion.node
		dataconn, err := conectar(direcNodo)
		if err != nil {
			fmt.Println("Error conectando al DATANODE\n")
			return
		}

		fmt.Fprintf(dataConn, "STORE %s %d\n", asignacion.block, len(data))
		
		respuesta, _ := leerLinea(bufio.NewReader(dataconn))
		if !strings.HasPrefix(respuesta, "OK") {
			fmt.Println("Datanode rechazó STORE\n")
			dataconn.Close()
			return
		}

		dataConn.Write(data)

		respuesta_nueva, _ := leerLinea(bufio.NewReader(dataConn))
		if !strings.HasPrefix(respuesta_nueva, "OK") {
			fmt.Println("Error guardando el bloque\n")
			dataConn.Close()
			return
		}

		dataConn.close()
	}

	fmt.Println("Archivo subido correctamente.\n")
}
