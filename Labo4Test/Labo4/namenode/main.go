package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func administrarConexion(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	msg, err := reader.ReadString("\n")
	if err != nil {
		log.Println("Error leyendo", err)
		return
	}

	fmt.Println("Namenode recibió:", msg)
	conn.Write([]byte("Mensaje recibido por el namenode!\n")
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
