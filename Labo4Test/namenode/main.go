package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"strconv"
)

var datanodes = []string {//hardcodeados
	"192.168.0.10:5000",
	"192.168.0.11:5001",
}

func ejecutarPut(partes []string, conn net.Conn) {
	nombre_archivo := partes[1]
	cant_bloques, err := strconv.Atoi(partes[2])
	if err != nil {
		conn.Write([]byte("NAMENODE ERROR: cantidad de bloques no valida\n"))
		return
	}

	fmt.Printf("NAMENODE: Recibi PUT %s con %d bloques\n", nombre_archivo, cant_bloques)

	for i:= 0; i < cant_bloques; i++ {
		dn := datanodes[i % len(datanodes)] //tipo RoundRobin
		linea := fmt.Sprintf("b%d %s\n", i, dn)
		conn.Write([]byte(linea))
	}

	conn.Write([]byte("END\n")) //por protocolo para que reciba el cliente
}


func ejecutarGet(partes []string, conn net.Conn) {
	nombre_archivo := partes[1]
	fmt.Printf("NAMENODE: Recibi GET de %s\n", nombre_archivo)
	//FALTA IMPLEMENTAR

}

func ejecutarLS(conn net.Conn) {
//FALTA IMPLEMENTAR
}

func ejecutarInfo(partes []string, conn net.Conn) {
//FALTA IMPLEMENTAR
}
func administrarConexion(conn net.Conn) {
	defer conn.Close() //ver si conviene cerrarla o esperar a que el cliente termine para enviar(que envie el ACK)
	reader := bufio.NewReader(conn)

	linea, err := reader.ReadString('\n')
	if err != nil {
		log.Println("NAMENODE: error leyendo el envio de CLIENT", err)
		return
	}
	
	linea = strings.TrimSpace(linea)
	partes := strings.Fields(linea)
	//el cliente YA verifica que los comandos se ingresaron bien

	comando := strings.ToLower(partes[0])

	switch comando {
	case "put":
		ejecutarPut(partes, conn)
	case "get":
		ejecutarGet(partes, conn)
	case "ls":
		ejecutarLS(conn)
	case "info":
		ejecutarInfo(partes, conn)
	default: conn.Write([]byte("Comando no valido\n"))//a la defensiva, no deberia pasar este caso
	}

	//fmt.Println("Namenode recibió:", msg)
	//conn.Write([]byte("Mensaje recibido por el namenode!\n"))
}

func main() {
	
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Namenode escuchando en puerto 8000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error aceptando conexión:", err)
			continue
		}

		go administrarConexion(conn)
	}

}
