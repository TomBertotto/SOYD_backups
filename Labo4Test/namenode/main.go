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
	"time"
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

func logear(msj string) {
	os.MkdirAll("logs", 0755)
	archivo, err := os.OpenFile("logs/namenode_log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("ERROR logeando")
		return
	}
	defer archivo.Close()
	
	timestamp := time.Now().Format("02/01 15:04")
	tiempoStr := fmt.Sprintf("[%s] %s\n", timestamp, msj)

	archivo.WriteString(tiempoStr)	
}

func cargarMetadata() map[string][]map[string]string {
	metadata := make(map[string][]map[string]string)
	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		return metadata
	}
	json.Unmarshal(contenido, &metadata)
	return metadata
}

func obtenerCargaDatanodes(metadata map[string][]map[string]string, datanodes []string) map[string]int {
	carga := make(map[string]int)

	for _, dn := range datanodes {
		carga[dn] = 0
	}

	for _, lista := range metadata {
		for _, entrada := range lista {
			dn := entrada["node"]
			carga[dn]++
		}
	}

	return carga

}

func elegirDatanodeMenorCarga(carga map[string]int) string {
	var elegido string
	min := int(^uint(0) >> 1 ) //i.e. MAX INT!

	for nodo, cant := range carga {
		if cant < min {
			min = cant
			elegido = nodo
		}
	}
	return elegido
}

func ejecutarPut(partes []string, conn net.Conn, reader *bufio.Reader) {
	nombre_archivo := partes[1]
	cant_bloques, err := strconv.Atoi(partes[2])
	if err != nil {
		logear("NAMENODE ERROR: cantidad de bloques no valida")
		conn.Write([]byte("NAMENODE ERROR: cantidad de bloques no valida\n"))
		return
	}
	
	logear(fmt.Sprintf("PUT recibido: %s con %d bloques", nombre_archivo, cant_bloques))
	fmt.Printf("NAMENODE: Recibi PUT %s con %d bloques\n", nombre_archivo, cant_bloques)

	metadata := cargarMetadata()
	carga := obtenerCargaDatanodes(metadata, datanodes)

	asignaciones[nombre_archivo] = []struct {
		Bloque string
		Node string
	}{}

	for i:= 0; i < cant_bloques; i++ {
		
		elegido := elegirDatanodeMenorCarga(carga)
		carga[elegido]++
		bloque_nro := fmt.Sprintf("b%d", i)
		asignaciones[nombre_archivo] = append(
			asignaciones[nombre_archivo],
			struct{ Bloque, Node string }{
				Bloque: bloque_nro,
				Node: elegido,
		},
		)
		
		linea := fmt.Sprintf("b%d %s\n", i, elegido)
		conn.Write([]byte(linea))
	}

	conn.Write([]byte("END\n")) //por protocolo para que reciba el cliente

	//------------ESPERA POR ACK-----------------
	ack_respuesta, err := reader.ReadString('\n')
	if err != nil {
		logear("ERROR leyendo ACK")
		fmt.Println("NAMENODE: error leyendo ACK", err)
		return
	}
	ack_respuesta = strings.TrimSpace(ack_respuesta)

	if ack_respuesta == "ACK" {
		logear("ACK RECIBIDO: actualizando metadata.json")
		fmt.Println("NAMENODE: ACK recibido, actualizando metadata.json")
		actualizarMetadata(nombre_archivo)
		conn.Write([]byte("OK\n"))
	} else if ack_respuesta == "ERROR" {
		logear("ERROR: no recibio ACK, PUT cancelado")
		fmt.Println("NAMENODE: no recibio ACK, PUT cancelado")
	}

	logear("Se cierra la conexion")
	fmt.Println("NAMENODE: cierra conexion despues de ACK")
	conn.Close()
}


func ejecutarGet(partes []string, conn net.Conn) {
	if len(partes) < 2 {
		conn.Write([]byte("NAMENODE: ERROR falta nombre archivo\n"))
		conn.Write([]byte("END\n"))
		return
	}

	nombre_archivo := partes[1]
	fmt.Println("NAMENODE: Recibi GET de archivo\n", nombre_archivo)
	logear("GET recibido, archivo: " + nombre_archivo)
	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		logear("ERROR leyendo metadata")
		conn.Write([]byte("NAMENODE: ERROR leyendo metadata\n"))
		conn.Write([]byte("END\n"))
		return
	}

	var metadata map[string][]map[string]string
	err = json.Unmarshal(contenido, &metadata)
	if err != nil {
		logear("ERROR parseando metadata")
		conn.Write([]byte("NAMENODE: ERROR parseando metadata\n"))
		conn.Write([]byte("END\n"))
		return
	}

	bloques, exito := metadata[nombre_archivo]

	if !exito {
		logear("ARCHIVO NO encontrado en metadata")
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
	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		logear("ERROR leyendo metadata.json")
		conn.Write([]byte("NAMENODE: Error leyendo METADATA\n"))
		conn.Write([]byte("END\n"))
		return
	}

	metadata := make(map[string][]map[string]string)
	json.Unmarshal(contenido, &metadata)

	for nombre := range metadata {
		conn.Write([]byte(nombre + "\n"))
	}

	conn.Write([]byte("END\n"))

}

func ejecutarInfo(partes []string, conn net.Conn) {
	nombre_archivo := partes[1]
	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		logear("ERROR leyendo metadata")
		conn.Write([]byte("NAMENODE: error leyendo METADATA\n"))
		conn.Write([]byte("END\n"))
		return
	}

	metadata := make(map[string][]map[string]string)
	json.Unmarshal(contenido, &metadata)

	bloques, existe := metadata[nombre_archivo]
	if !existe {
		logear("El archivo no existe: " + nombre_archivo)
		conn.Write([]byte("NO_EXISTE\n"))
		conn.Write([]byte("END\n"))
		return
	}

	for _, entrada := range bloques {
		linea := fmt.Sprintf("%s %s\n", entrada["block"], entrada["node"])
		conn.Write([]byte(linea))
	}

	conn.Write([]byte("END\n"))
}

func ejecutarRM(partes []string, conn net.Conn, reader *bufio.Reader) {
	if len(partes) < 2 {
		logear("ERROR: falta nombre archivo")
		conn.Write([]byte("NAMENODE: Error falta nombre archivo\nEND\n"))
		return
	}

	nombre_archivo := partes[1]

	contenido, err := os.ReadFile("metadata.json")
	if err != nil {
		logear("ERROR leyendo metadata")
		conn.Write([]byte("ERROR leyenedo metadata\nEND\n"))
		return
	}

	metadata := make(map[string][]map[string]string)
	json.Unmarshal(contenido, &metadata)

	bloques, existe := metadata[nombre_archivo]
	if !existe {
		logear("No existe el archivo")
		conn.Write([]byte("NO_EXISTE\nEND\n"))
		return
	}

	for _, entrada := range bloques {
		linea := fmt.Sprintf("%s %s\n", entrada["block"], entrada["node"])
		conn.Write([]byte(linea))
	}
	conn.Write([]byte("END\n"))

	ack, err := reader.ReadString('\n')
	if err != nil {
		logear("ERROR leyendo ACK")
		fmt.Println("NAMENODE: error leyendo ACK:", err)
		return
	}

	ack = strings.TrimSpace(ack)

	if ack == "ACK" {
		delete(metadata, nombre_archivo)
		nuevo, _ := json.MarshalIndent(metadata, "", " ")
		os.WriteFile("metadata.json", nuevo, 0644)
		logear("se actualizo metadata.json --- archivo eliminado")
		fmt.Println("NAMENODE: metadata.json actualizado (archivo eliminado)")
		conn.Write([]byte("OK\n"))
	}

	conn.Close()//chequear
}

func administrarConexion(conn net.Conn) {
	reader := bufio.NewReader(conn)

	linea, err := reader.ReadString('\n')
	if err != nil {
		logear("ERROR leyendo el envio de CLIENTE")
		log.Println("NAMENODE: error leyendo el envio de CLIENTE", err)
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
	case "rm":
		ejecutarRM(partes, conn, reader)
	default: conn.Write([]byte("Comando no valido\n"))//a la defensiva, no deberia pasar este caso
	}

	//fmt.Println("Namenode recibió:", msg)
	//conn.Write([]byte("Mensaje recibido por el namenode!\n"))
}

func main() {
	
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		logear("ERROR al escuchar en el puerto")
		log.Fatal(err)
	}

	fmt.Println("NAMENODE escuchando en puerto 8000")
	logear("NAMENODE escuchando en puerto 8000")
	for {
		conn, err := listener.Accept()
		if err != nil {
			logear("ERROR aceptando conexion")
			fmt.Println("Error aceptando conexión:", err)
			continue
		}

		go administrarConexion(conn)
	}

}
