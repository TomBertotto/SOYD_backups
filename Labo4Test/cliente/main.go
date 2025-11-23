package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"strconv"
	"os"
	"io"
	"time"
)


const BLOCK_TAMANIO = 1024

func generarID(nombre_archivo string, bloque int) string {
	base := strings.TrimSuffix(nombre_archivo, ".txt")
	return fmt.Sprintf("%s_b%d.txt", base, bloque)
}

func dividirEnBloques(archivo []byte) [][]byte {
	var bloques[][]byte
	for i:= 0; i < len(archivo); i+=BLOCK_TAMANIO {
		fin := i + BLOCK_TAMANIO
		if fin > len(archivo) {
			fin = len(archivo)
		}
		bloques = append(bloques, archivo[i:fin])
	}
	return bloques
}

func logear(msj string) {
	os.MkdirAll("logs", 0755)
	archivo, err := os.OpenFile("logs/cliente_log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("ERROR logeando")
		return
	}
	defer archivo.Close()
	
	timestamp := time.Now().Format("02/01 15:04")
	tiempoStr := fmt.Sprintf("[%s] %s\n", timestamp, msj)

	archivo.WriteString(tiempoStr)	
}

func enviarBloqueADatanode(addr string, blockID string, data[]byte) error {
	logear(fmt.Sprintf("CLIENTE: conectando con DATANODE %s", addr))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error al conectar a DATANODE"))
		fmt.Println("CLIENTE: error al conectar a DATANODE:", err)
		return err
	}
	defer conn.Close()
	
	fmt.Fprintf(conn, "store %s %d\n", blockID, len(data)) // formato <store> <nombre_b0.txt> <size>	
	logear(fmt.Sprintf("CLIENTE -> DATANODE %s: store %s (%d bytes)", addr, blockID, len(data)))

	_, err = conn.Write(data)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error enviando bloque"))	
		fmt.Println("CLIENTE: error enviando bloque:", err)
		return err
	}

	logear(fmt.Sprintf("CLIENTE: bloque %s enviado a %s", blockID, addr))
	return nil
}


func ejecutarPut(nombre_archivo string, addrNamenode string) {
	archivo, err := os.ReadFile(nombre_archivo)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error leyendo archivo local"))
		fmt.Println("Error abriendo archivo:", err)
		return
	}
	
	bloques := dividirEnBloques(archivo)
	cant_bloques := len(bloques)
	
	fmt.Printf("Archivo: %s (%d bytes) -> %d bloques de 1KB\n", nombre_archivo, len(archivo), cant_bloques)

	msg := fmt.Sprintf("put %s %d\n", nombre_archivo, cant_bloques)



	conn, err := net.Dial("tcp", addrNamenode) //me conecto al namenode
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error conectando al NAMENODE"))
		fmt.Println("Error conectado con el namenode:", err)
		return
	}
	defer conn.Close()
	logear(fmt.Sprintf("CLIENTE conectado al NAMENODE"))
	
	conn.Write([]byte(msg))
	
	fmt.Println("Cliente esperando asignacion de datanodes...")
	
	reader := bufio.NewReader(conn)
	asignaciones_bloques := make(map[int]string)

	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			logear(fmt.Sprintf("CLIENTE: error al recibir del namenode"))
			fmt.Println("Error al recibir del namenode")
			break
		}
		linea = strings.TrimSpace(linea)

		if linea == "END" { //establezco como protocolo que el namenode va a usar END al final
			break
		}

		logear(fmt.Sprintf("CLIENTE <- NAMENODE: " + linea))
		
		fmt.Println("---", linea, "---")
		partes := strings.Fields(linea)
		if len(partes) == 2 {
			blockIDStr := strings.TrimPrefix(partes[0], "b")
			blockID, err := strconv.Atoi(blockIDStr)
			if err == nil {
				asignaciones_bloques[blockID] = partes[1]
			}
		}
	}

	fmt.Println("CLIENTE: enviando datos a DATANODES")

	for bloqueID, addrDatanode := range asignaciones_bloques {
		data := bloques[bloqueID]
		logear(fmt.Sprintf("Enviando bloque %d a %s", bloqueID, addrDatanode))
		fmt.Printf("Enviando bloque %d a %s\n", bloqueID, addrDatanode)
		id_bloque := generarID(nombre_archivo, bloqueID)//CONVENCION: tomo que el id es nombre_archivo_b0.txt, nombre_archivo_b1.txt...
		err := enviarBloqueADatanode(addrDatanode, id_bloque, data) //me conecto a los datanodes
		if err != nil {
			logear("ERROR enviando BLOQUE: PUT cancelado")
			fmt.Printf("CLIENTE: error enviando el bloque %d -> %s\n", bloqueID, addrDatanode)
			fmt.Println("PUT cancelado")
			fmt.Fprintf(conn, "ERROR\n")
			return			
		}
	}
	
	fmt.Println("CLIENTE: se completo la transferencia")
	fmt.Println("CLIENTE: enviando ACK al NAMENODE")
	fmt.Fprintf(conn, "ACK\n")
}

func pedirBloqueAlDatanode(addrDatanode, nombre_archivo string, bloque int) ([]byte, error) {
	conn, err := net.Dial("tcp", addrDatanode)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error conectando al DATANODE"))
		return nil, fmt.Errorf("CLIENTE: error conectando al DATANODE: %v",err)
	}

	defer conn.Close()

	blockID:= generarID(nombre_archivo, bloque)

	fmt.Fprintf(conn,"read %s\n", blockID) //envio el read al datanode

	reader := bufio.NewReader(conn)

	logear(fmt.Sprintf("CLIENTE -> DATANODE %s", addrDatanode))
	linea, err := reader.ReadString('\n')
	if err != nil {
		logear(fmt.Sprintf("ERROR leyendo linea"))
		fmt.Println("Error leyendo linea:", err)
		return nil, err
	}
	linea = strings.TrimSpace(linea)
	
	size, err := strconv.Atoi(linea)
	if err != nil {
		fmt.Println("CLIENTE: Tamaño invalido en DATANODE")
		return nil, err
	}
	data := make([]byte, size)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error leyendo datos del bloque"))
		return nil, fmt.Errorf("CLIENTE: error leyendo datos del bloque: %v", err)
	}

	return data, nil

}


func ejecutarGetCat(nombre_archivo string, addrNamenode string, comando string) {
	conn, err := net.Dial("tcp", addrNamenode)
	if err != nil {
		logear(fmt.Sprintf("No se pudo conectar al namenode"))
		fmt.Println("No se pudo conectar al namenode:", err)
		return
	}
	
	logear(fmt.Sprintf("CLIENTE -> NAMENODE: get %s", nombre_archivo))
	defer conn.Close()

	fmt.Fprintf(conn, "get %s\n", nombre_archivo)

	reader := bufio.NewReader(conn)

	bloques := make(map[int]string)

	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("CLIENTE: error leyendo respuesta namenode:", err)
			return
		}

		linea = strings.TrimSpace(linea)
		if linea == "END" { //por protocolo aseguro que namenode envia END al final
			break
		}

		partes := strings.Fields(linea)

		if len(partes) != 2 {
			fmt.Println("CLIENTE: recibio respuesta no valida")
			return
		}

		bloqueStr := strings.TrimPrefix(partes[0], "b")
		bloqueNum, err := strconv.Atoi(bloqueStr)
		if err != nil {
			fmt.Println("CLIENTE: error procesando nro bloque:", err)
			return
		}

		ipDatanode := partes[1]
		bloques[bloqueNum] = ipDatanode
		logear(fmt.Sprintf("CLIENTE <- NAMENODE: " + linea))
		
	}

	if len(bloques) == 0 {
		fmt.Println("El archivo NO existe en el DFS")
		return
	}

	var resultado[]byte

	for i:= 0; i < len(bloques); i++ {
		ip := bloques[i]
		data, err := pedirBloqueAlDatanode(ip, nombre_archivo, i)
		if err != nil {
			fmt.Printf("CLIENTE: error obteniendo bloque %d: %v\n",i,err)
			return
		}
		resultado = append(resultado, data...)//ver
	}

	if comando != "cat" {
		err = os.WriteFile(nombre_archivo, resultado, 0644)
		
		if err != nil {
			fmt.Println("CLIENTE: error al escribir el archivo localmente:", err)
			return
		}

		logear(fmt.Sprintf("Archivo %s descargado con éxito", nombre_archivo))
		fmt.Println("Archivo descargado con éxito: ", nombre_archivo)
	} else {
		fmt.Print(string(resultado))
		fmt.Println()
		return
	}
}



func ejecutarInfo(nombre_archivo string, addrNamenode string) {
	conn, err := net.Dial("tcp", addrNamenode)
	if err != nil {
		fmt.Println("CLIENTE: error conectandose al NAMENODE:", err)
		return
	}
	defer conn.Close()

	fmt.Fprintf(conn, "info %s\n", nombre_archivo)

	reader := bufio.NewReader(conn)

	fmt.Printf("Información del archivo %s: \n", nombre_archivo)

	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("CLIENTE: error leyendo linea INFO:", err)
			return
		}

		linea = strings.TrimSpace(linea)

		if linea == "END" {
			break
		}

		if linea == "NO_EXISTE" {
			fmt.Println("El archivo NO está en el DFS")
			return
		}

		partes := strings.Fields(linea)
		if len(partes) == 2 {
			fmt.Printf(" ----- Bloque %s en %s\n", partes[0], partes[1])
		}
	}
}


func ejecutarLS(comando string, addrNamenode string) {
	conn, err := net.Dial("tcp", addrNamenode)
	if err != nil {
		fmt.Println("CLIENTE: error conectando al NAMENODE:", err)
		return
	}
	defer conn.Close()
	
	fmt.Fprintf(conn, "%s\n", comando)
	reader := bufio.NewReader(conn)

	fmt.Println("Archivos en el DFS:")

	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("CLIENTE: error leyendo linea LS", err)
			return
		}
		linea = strings.TrimSpace(linea)
		if linea == "END" {
			break
		}
		fmt.Println(" -----", linea)
	}

}

func ejecutarRM(nombre_archivo string, addrNamenode string) {
	conn, err := net.Dial("tcp", addrNamenode)
	if err != nil {
		logear(fmt.Sprintf("CLIENTE: error conectando al NAMENODE"))
		fmt.Println("CLIENTE: error conectando al NAMENODE:", err)
		return
	}

	defer conn.Close()

	fmt.Fprintf(conn, "rm %s\n", nombre_archivo)
	reader := bufio.NewReader(conn)

	bloques := make(map[int]string)

	for {
		linea, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("CLIENTE: error leyendo respuesta RM:", err)
			return
		}

		linea = strings.TrimSpace(linea)

		if linea == "END" {
			break
		}

		if linea == "NO_EXISTE" {
			fmt.Println("CLIENTE: archivo no encontrado en el DFS")
			return
		}

		partes := strings.Fields(linea)
		if len(partes) != 2 {
			continue
		}

		bloqueStr := strings.TrimPrefix(partes[0], "b")
		bloqueNum, _ := strconv.Atoi(bloqueStr)
		bloques[bloqueNum] = partes[1]
	}

	for bloque, ip := range bloques {
		blockID := generarID(nombre_archivo, bloque)

		fmt.Printf("CLIENTE: pidiendo borrar bloque %s en %s\n", blockID, ip)

		connDN, err := net.Dial("tcp", ip)
		if err != nil {
			logear(fmt.Sprintf("CLIENTE: error conectando al DATANODE"))
			fmt.Println("CLIENTE: error conectando al DATANODE:", ip)
			return
		}
		fmt.Fprintf(connDN, "delete %s\n", blockID)
		connDN.Close()
	}

	fmt.Fprintf(conn, "ACK\n")

	linea, _ := reader.ReadString('\n')
	linea = strings.TrimSpace(linea)
	if linea == "OK" {
		logear(fmt.Sprintf("CLIENTE: archivo eliminado exitosamente del DFS"))
		fmt.Println("CLIENTE: archivo eliminado exitosamente del DFS")
	}
}

func procesarComando(input string, addrNamenode string) {
	partes := strings.Fields(input)
	comando := strings.ToLower(partes[0])
	logear(fmt.Sprintf("SE INGRESO COMANDO: %s", comando))

	switch comando {
	case "put": 
		if len(partes) < 2 { //ver si se puede refactorizar para no tener tanto codigo de conexion duplicado
			fmt.Println("Incorrecto, uso: put <archivo>")
			return
		}

		ejecutarPut(partes[1], addrNamenode)
	
	case "get":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: get <archivo>")
			return
		}
		ejecutarGetCat(partes[1], addrNamenode, comando)	
	
	case "ls":
		ejecutarLS(comando, addrNamenode)
	case "info":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: info <archivo>")
			return
		}
		ejecutarInfo(partes[1], addrNamenode)
	case "rm":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: rm <archivo>")
			return
		}
		ejecutarRM(partes[1], addrNamenode)
	case "cat":
		if len(partes) < 2 {
			fmt.Println("Incorrecto, uso: cat <archivo>")
			return
		}
		ejecutarGetCat(partes[1], addrNamenode, comando)
	default: fmt.Println("Comando no válido:", comando)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Parametros incorrectos")
		fmt.Println("Formato: go run client.go <IP_namenode>:<puerto>")
		return
	}

	logear(fmt.Sprintf("CLIENTE: comienza el programa"))
	addrNamenode := os.Args[1]
	reader:= bufio.NewReader(os.Stdin)

	fmt.Println("----------CLIENTE DFS----------")
	fmt.Println("Comandos: put <archivo>, get<archivo>, ls, info <archivo>, exit")

	for {
		fmt.Println()
		fmt.Println()
		fmt.Print("dfs> ")
		
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error leyendo la entrada:", err)
			continue
		}
		
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		if input == "exit" {
			logear(fmt.Sprintf("CLIENTE: salida del programa"))
			fmt.Println("Saliendo del cliente...")
			return
		}

		procesarComando(input, addrNamenode)
	}
}


