package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"encoding/json"
	"strings"
	"os"
	"strconv"
)

var datanodes = []string {//hardcodeados
	"192.168.0.5:5000",
	"192.168.0.24:5000",
}

var asignaciones = make(map[string][]struct {
	Bloque string //formato b0,b1...
	Node string //formato IP:PUERTO
})


func actualizarMetadata(nombre_archivo string) {
	metadata := make(map[string][]map[string]string)

	contenido, err := os.ReadFile("metadata.json")
	if err == nil {
		json.Unmarshal(contenido, &metadata)
	}

	var bloques []map[string]string

	for _, info := range asignaciones[nombre_archivo] {
		bloques = append(bloques, map[string]string{
			"block": info.Bloque,
			"node": info.Node,
		})
	}

	metadata[nombre_archivo] = bloques

	nuevo, _ := json.MarshalIndent(metadata, "", " ")
	os.WriteFile("metadata.json", nuevo, 0644)
	fmt.Println("NAMENODE: metadata.json actualizado")
}


func ejecutarPut(partes []string, conn net.Conn, reader *bufio.Reader) {
	nombre_archivo := partes[1]
	cant_bloques, err := strconv.Atoi(partes[2])
	if err != nil {
		conn.Write([]byte("NAMENODE ERROR: cantidad de bloques no valida\n"))
		return
	}
	

	fmt.Printf("NAMENODE: Recibi PUT %s con %d bloques\n", nombre_archivo, cant_bloques)

	asignaciones[nombre_archivo] = []struct {
		Bloque string
		Node string
	}{}

	for i:= 0; i < cant_bloques; i++ {
		dn := datanodes[i % len(datanodes)] //tipo RoundRobin
		
		bloque_nro := fmt.Sprintf("b%d", i)
		asignaciones[nombre_archivo] = append(
			asignaciones[nombre_archivo],
			struct{ Bloque, Node string }{
				Bloque: bloque_nro,
				Node: dn,
		},
		)
		
		linea := fmt.Sprintf("b%d %s\n", i, dn)
		conn.Write([]byte(linea))
	}

	conn.Write([]byte("END\n")) //por protocolo para que reciba el cliente

	//------------ESPERA POR ACK-----------------
	ack_respuesta, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("NAMENODE: error leyendo ACK", err)
		return
	}
	ack_respuesta = strings.TrimSpace(ack_respuesta)

	if ack_respuesta == "ACK" {
		fmt.Println("NAMENODE: ACK recibido, actualiznado metadata.json")
		actualizarMetadata(nombre_archivo)
		conn.Write([]byte("OK\n"))
	} else {
		fmt.Println("NAMENODE: no recibio ACK")
	}

	fmt.Println("NAMENODE: cierrta conexion despues de ACK")
	conn.Close()
}


func ejecutarGet(partes []string, conn net.Conn) {
	if len(partes) < 2 {
		conn.Write([]byte("NAMENODE: ERROR falta nombre archivo\n"))
		conn.Write([]byte("END\n"))
		return
	}

	nombre_archivo := partes[1]
	fmt.Println("NAMENODE: Recibi GET de archivo %s\n", nombre_archivo)

	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		conn.Write([]byte("NAMENODE: ERROR leyendo metadata\n"))
		conn.Write([]byte("END\n"))
		return
	}

	var metadata map[string][]map[string]string
	err = json.Unmarshal(contenido, &metadata)
	if err != nil {
		conn.Write([]byte("NAMENODE: ERROR parseando metadata\n"))
		conn.Write([]byte("END\n"))
		return
	}

	bloques, exito := metadata[nombre_archivo]

	if !exito {
		conn.Write([]byte("NAMENODE: ERROR archivo NO encontrado en metadata\n"))
		conn.Write([]byte("END\n"))
		return		
	}

	for _, entrada := range bloques {
		linea := fmt.Sprintf("%s %s\n", entrada["block"], entrada["node"])
		conn.Write([]byte(linea))
	}
	conn.Write([]byte("END\n"))
}

func ejecutarLS(conn net.Conn) {
//FALTA IMPLEMENTAR
}

func ejecutarInfo(partes []string, conn net.Conn) {
//FALTA IMPLEMENTAR
}
func administrarConexion(conn net.Conn) {
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
		ejecutarPut(partes, conn, reader)
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
