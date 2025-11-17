package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

const BLOCKS_DIR = "blocks/"

func asegurarDirectorioBlocks() error {
	if _, err := os.Stat(BLOCKS_DIR); os.IsNotExist(err) {
		return os.Mkdir(BLOCKS_DIR, 0755)
	}
	return nil
}

func guardarBlock(blockID string, data []byte) error {
	nombre_archivo := BLOCKS_DIR + blockID
	return os.WriteFile(nombre_archivo, data, 0644)
}

func leerBlock(blockID string) ([]byte, error) {
	nombre_archivo := BLOCKS_DIR + blockID
	return os.ReadFile(nombre_archivo)
}



func administrarConexion(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	commandLine, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error leyendo comando\n")
		return
	}

	commandLine = strings.TrimSpace(commandLine)
	partes := strings.Fields(commandLine)

	if len(partes) < 2 {
		conn.Write([]byte("ERROR: comando no válido\n"))
		return
	}

	comando := strings.ToUpper(partes[0])
	blockID := partes[1]

	switch comando {
	case "STORE":
		if len(partes) != 3 {
			conn.Write([]byte("ERROR: mal formato de STORE"))
			return
		}
		size, err := strconv.Atoi(partes[2])
		if err != nil || size <= 0 {
			conn.Write([]byte("ERROR: tamaño no válido\n"))
			return
		}

		conn.Write([]byte("OK\n"))

		data := make([]byte, size)

		_, err = io.ReadFull(reader, data)

		if err != nil {
			conn.Write([]byte("ERROR leyendo datos del bloque\n"))
			return
		}

		if err := guardarBlock(blockID, data); err != nil {
			conn.Write([]byte("ERROR guardando el bloque\n"))
			return
		}

		conn.Write([]byte("OK STORE\n"))
		log.Printf("STORE: Bloque %s guardado (%d bytes)\n", blockID, size)
	
	case "READ":
		data, err := leerBlock(blockID)
		if err != nil {
			conn.Write([]byte("ERROR: bloque no encontrado\n"))
			return
		}

		conn.Write([]byte(fmt.Sprintf("OK %d\n", len(data))))
		conn.Write(data)

		log.Printf("READ: Bloque %s enviado (%d bytes)\n", blockID, len(data))

	default:
		conn.Write([]byte("ERROR: comando no válido/desconocido\n"))
	}
}


func conectarDatanode(puerto string) error {
	if err := asegurarDirectorioBlocks(); err != nil {
		return fmt.Errorf("No se pudo crear blocks/\n")
	}

	ln, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		return fmt.Errorf("Error escuchando puerto %s: %v\n", puerto, err)
	}

	log.Printf("Datanode escuchando en puerto %s\n", puerto);

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error aceptando la conexión: %v\n", err)
			continue
		}
		go administrarConexion(conn)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Formato incorrecto, debe ser: go run datanode.go <puerto>\n")
		return
	}

	puerto := os.Args[1]
	log.Fatal(conectarDatanode(puerto))
}
