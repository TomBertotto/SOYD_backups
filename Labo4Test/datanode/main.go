package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"strconv"
	"time"
)

const bloquesDir = "./blocks/"
const BLOQUE_TAMANIO = 1024

//so2025YD..

func logear(msj string) {
	os.MkdirAll("logs", 0755)
	archivo, err := os.OpenFile("logs/datanode_log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("ERROR logeando")
		return
	}
	defer archivo.Close()
	
	timestamp := time.Now().Format("02/01 15:04")
	tiempoStr := fmt.Sprintf("[%s] %s\n", timestamp, msj)

	archivo.WriteString(tiempoStr)	
}

func almacenarBloque(reader *bufio.Reader, blockID string, size int) {
	ruta := bloquesDir + blockID
	archivo, err := os.Create(ruta)
	if err != nil {
		logear("ERROR creando archivo")
		fmt.Println("DATANODE: error creando archivo:", err)
		return
	}
	defer archivo.Close()

	buffer := make([]byte, size)
	_, err = io.ReadFull(reader, buffer)
	if err != nil {
		logear("ERROR leyendo bloque del buffer")
		fmt.Println("DATANODE: error leyendo el bloque del buffer")
		return
	}

	archivo.Write(buffer)
	fmt.Println("Bloque ", blockID, " almacenado")
}

func enviarBloque(conn net.Conn, blockID string) {
	ruta := bloquesDir + blockID
	data, err := os.ReadFile(ruta)
	if err != nil {
		logear("ERROR leyendo bloque con READ, bloque: " + blockID)
		fmt.Println("DATANODE: error leyendo bloque con READ, bloque:", blockID)
		return
	}
	//envio el tamanio al cliente
	fmt.Fprintf(conn, "%d\n", len(data))

	conn.Write(data)
	fmt.Println("Bloque ", blockID, "enviado")
}

func eliminarBloque(conn net.Conn, blockID string) {
	ruta := bloquesDir + blockID
	err := os.Remove(ruta)
	if err != nil {
		logear("ERROR eliminando bloque :"+ blockID)
		fmt.Println("DATANODE: error eliminando bloque:", blockID, err)
		fmt.Fprintf(conn, "ERROR\n")
		return
	}
	logear("Bloque eliminado: " + blockID)
	fmt.Println("DATANODE: bloque eliminado:", blockID)
	fmt.Fprintf(conn, "OK\n")
}

func administrarConexion(conn net.Conn) {
	
	defer conn.Close()
	reader := bufio.NewReader(conn)
	linea, err := reader.ReadString('\n')
	partes := strings.Fields(linea)
	if err != nil || len(partes) < 2 {
		fmt.Println("DATANODE: error leyendo comando")
		return
	}
	
	comando := partes[0]
	blockID := partes[1]

	switch comando {
	case "store":
		if len(partes) < 3 {
			fmt.Println("DATANODE: error en cantidad de argumentos STORE")
			return
		}
		sizeStr := strings.TrimSpace(partes[2])
		sizeStr = strings.TrimSuffix(sizeStr, "\r")
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			fmt.Println("DATANODE: error procesando SIZE")
			return
		}
		almacenarBloque(reader, blockID, size)
	case "read":
		enviarBloque(conn, blockID)
	case "delete":
		eliminarBloque(conn, blockID)
	}
}


func main() {

	os.MkdirAll(bloquesDir, 0755)
	
	listener, err := net.Listen("tcp", ":5000")//puerto hardcodeado
	if err != nil {
		logear("ERROR al escuchar en el puerto establecido")
		fmt.Println("DATANODE: error al escuchar en el puerto establecido")
		return
	}

	fmt.Println("DATANODE: escuchando en el puerto 5000")
	logear("Escuchando en el puerto 5000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go administrarConexion(conn)
	}
}
