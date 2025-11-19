package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Parametros incorrectos")
		fmt.Println("Formato: go run client.go <IP_namenode>:<puerto>")
		return
	}

	addrNamenode := os.Args[1]
	
	conn, err := net.Dial("tcp", addrNamenode)

	if err != nil {
		log.Fatal("No se pudo conectar al namenode:", err)
	}
	defer conn.Close()

	fmt.Println("Conectando al Namenode:", addrNamenode)
	conn.Write([]byte("Hola desde el cliente!\n"))

	respuesta, err := bufio.NewReader(conn).ReadString("\n")
	if err != nil {
		log.Fatal("Error leyendo respuesta:", err)
	}

	fmt.Println("Respuesta del namenode:", respuesta)
}
