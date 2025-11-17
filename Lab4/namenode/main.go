package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

const (
	NAMENODE_PORT = "8000"
	METADATA_FILE = "metadata.json"
	BLOCK_SIZE = 1024
)

type BlockInfo struct {
	Block string `json:"block"`
	Node string `json:"node"`
}

type Metadata map[string][]BlockInfo


type Namenode struct {
	port string
	metadata Metadata
	datanodes []string
	mu sync.Mutex
}

func crearNamenode(puerto string, datanodes []string) *Namenode {
	return &Namenode {
		port: puerto,
		metadata: make(Metadata),
		datanodes: datanodes,
	}
}

func (namenode *Namenode) cargarMetadata() error {
	file, err := os.Open(METADATA_FILE)
	if os.IsNotExist(err) {
		log.Println("Archivo metadata.json no existe")
		return nil
	}

	if err != nil {
		return fmt.Errorf("error al abrir metadata.json\n")
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&namenode.metadata); err != nil {
		return fmt.Errorf("error decodificando metadata\n")
	}

	log.Printf("Metadata cargada: %d archivos registrados\n", len(namenode.metadata))
	return nil
}

func (namenode *Namenode) guardarMetadata() error {
	namenode.mu.Lock()
	defer namenode.mu.Unlock()

	file, err := os.Create(METADATA_FILE)
	if err != nil {
		return fmt.Errorf("error creando metadata\n")
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(namenode.metadata); err != nil {
		return fmt.Errorf("error codificando metadata\n")
	}

	return nil
}


func (namenode *Namenode) iniciarServidorNamenode() error {
	if err := namenode.cargarMetadata(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", ":"+namenode.port)
	if err != nil {
		return fmt.Errorf("error iniciando el servidor\n")
	}

	defer listener.Close()

	log.Printf("Namenode escuchando en puerto %s\n", namenode.port)
	log.Printf("Datanodes disponibles: %v\n", namenode.datanodes)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error aceptando conexión\n")
			continue
		}

		go namenode.conectarCliente(conn)
	}
}

func (namenode *Namenode) conectarCliente(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	commandLine, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error leyendo comando\n")
		return
	}

	commandLine = strings.TrimSpace(commandLine)
	parts := strings.Fields(commandLine)

	if len(parts) == 0 {
		conn.Write([]byte("ERROR: comando vacío\n"))
		return
	}

	command := strings.ToUpper(parts[0])
	log.Printf("Comando recibido: %s\n", commandLine)

	switch command {
	case "LS":
		namenode.ejecutarLS(conn)
	case "INFO":
		namenode.ejecutarInfo(conn, parts)
	case "PUT":
		namenode.ejecutarPut(conn, parts)
	case "GET":
		namenode.ejecutarGet(conn, parts)
	default:
		conn.Write([]byte(fmt.Sprintf("ERROR: comando no válido %s\n", command)))
	}
}

func (namenode *Namenode) ejecutarLS(conn net.Conn) {
	namenode.mu.Lock()
	defer namenode.mu.Unlock()

	if len(namenode.metadata) == 0 {
		conn.Write([]byte("OK 0\n"))
		return
	}

	respuesta := fmt.Sprintf("OK %d\n", len(namenode.metadata))
	for nombre_archivo := range namenode.metadata {
		respuesta += nombre_archivo + "\n"
	}

	conn.Write([]byte(respuesta))
	log.Printf("LS: %d archivos listados", len(namenode.metadata))
}


func (namenode *Namenode) ejecutarInfo(conn net.Conn, partes []string) {
	if len(partes) < 2 {
		conn.Write([]byte("ERROR en INFO\n"))
		return
	}

	nombre_archivo := partes[1]

	namenode.mu.Lock()
	blocks, existe := namenode.metadata[nombre_archivo]
	namenode.mu.Unlock()

	if !existe {
		conn.Write([]byte(fmt.Sprintf("Error: archivo %s no existe\n", nombre_archivo)))
		return
	}

	respuesta := fmt.Sprintf("OK %d\n", len(blocks))
	for _, block := range blocks {
		respuesta += fmt.Sprintf("%s %s\n", block.Block, block.Node)
	}

	conn.Write([]byte(respuesta))
	log.Printf("INFO: información de '%s' enviada (%d bloques)\n", nombre_archivo, len(blocks))
}

func (namenode *Namenode) ejecutarPut(conn net.Conn, partes []string) {
	if len(partes) < 3 {
		conn.Write([]byte("ERROR en formato de PUT"))
		return
	}

	nombre_archivo := partes[1]
	var numBlocks int
	_, err := fmt.Sscanf(partes[2], "%d", &numBlocks)
	if err != nil {
		conn.Write([]byte("ERROR: numero de bloques no válido\n"))
		return
	}	

	if numBlocks <= 0 {
		conn.Write([]byte("ERROR: numero de bloques debe ser mayor a 0\n"))
		return		
	}

	log.Printf("PUT: asignando %d bloques para '%s'", numBlocks, nombre_archivo)

	blocks := make([]BlockInfo, numBlocks)
	respuesta := "OK\n"

	for i := 0; i < numBlocks; i++ {
		blockID := fmt.Sprintf("%s_b%d", nombre_archivo, i) //ver formato de archivo como queda
		datanodeIndex := i % len(namenode.datanodes)
		datanode := namenode.datanodes[datanodeIndex]
		
		blocks[i] = BlockInfo{
			Block: blockID,
			Node: datanode,
		}
		respuesta += fmt.Sprintf("%s %s\n", blockID, datanode)
	}

	namenode.mu.Lock()
	namenode.metadata[nombre_archivo] = blocks
	namenode.mu.Unlock()

	if err := namenode.guardarMetadata(); err != nil {
		log.Printf("error guardando metadata\n")
		conn.Write([]byte(fmt.Sprintf("ERROR: %v\n", err)))
		return
	}

	conn.Write([]byte(respuesta))
	log.Printf("PUT: '%s' registrado con %d bloques", nombre_archivo, numBlocks)
}

func (namenode *Namenode) ejecutarGet(conn net.Conn, partes []string) {
	if len(partes) < 2 {
		conn.Write([]byte("ERROR en el uso de GET\n"))
		return
	}

	nombre_archivo := partes[1]

	namenode.mu.Lock()
	blocks, existe := namenode.metadata[nombre_archivo]
	namenode.mu.Unlock()

	if !existe {
		conn.Write([]byte(fmt.Sprintf("ERROR: archivo %s no existe\n", nombre_archivo)))
		return
	}

	respuesta := fmt.Sprintf("OK %d\n", len(blocks))

	for _, block := range blocks {
		respuesta += fmt.Sprintf("%s %s\n", block.Block, block.Node)
	}

	conn.Write([]byte(respuesta))
	log.Printf("GET: ubicación de '%s' enviada (%d bloques)\n", nombre_archivo, len(blocks))
}


func main() {


	datanodes := []string { //hardcodeados
		"localhost:5001",
		"localhost:5002",
		"localhost:5003",
		"localhost:5004",
	}

	namenode := crearNamenode(NAMENODE_PORT, datanodes)
	log.Fatal(namenode.iniciarServidorNamenode())

}
