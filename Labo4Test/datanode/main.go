package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const bloquesDir = "./blocks/"
const BLOQUE_TAMANIO = 1024

func almacenarBloque(reader *bufio.Reader, blockID string) {
	ruta := bloquesDir + blockID
	archivo, err := os.Create(ruta)
	if err != nil {
		fmt.Println("DATANODE: error creando archivo:", err)
		return
	}
	defer archivo.Close()

	buffer := make([]byte, BLOQUE_TAMANIO)
	_, err = io.ReadFull(reader, buffer)
	if err != nil {
		fmt.Println("DATANODE: error leyendo el bloque del buffer")
		return
	}

	file.Write(buffer)
	fmt.Println("Bloque ", blockID, " almacenado")
}

func enviarBloque(conn net.Conn, blockID string) {
	ruta := bloquesDir + blockID
	data, er := os.ReadFile(ruta)
	if err != nil {
		fmt.Println("DATANODE: error leyendo bloque con READ, bloque:", blockID)
		return
	}
	conn.Write(data)
	fmt.Println("Bloque ", blockID, "enviado")
}

func administrarConexion(conn net.Conn) {
	
	defer conn.Close()
	reader := bufio.NewReader(conn)
	linea, err := reader.ReadString('\n')
	partes := strings.Fields(linea)
	if err != nil || len(partes) < 2{
		fmt.Println("DATANODE: error leyendo comando")
		return
	}
	
	comando := partes[0]
	blockID := partes[1]

	switch comando {
	case "store":

		almacenarBloque(reader, blockID)
	case "read":
		enviarBloque(conn, blockID)
	}
}


func main() {

	os.MkdirAll(bloquesDir, 0755)
	
	listener, err := net.Listen("tcp", ":5000")//puerto hardcodeado
	if err != nil {
		fmt.Println("DATANODE: error al escuchar en el puerto establecido")
		return
	}

	fmt.Println("DATANODE: escuchando en el puerto 5000")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go administrarConexion(conn)
	}
}
