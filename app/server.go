package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Did accept connection")
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}
	_ = binary.BigEndian.Uint16(buffer[0:4])
	_ = binary.BigEndian.Uint16(buffer[4:6])
	_ = binary.BigEndian.Uint16(buffer[6:8])
	correlationId := binary.BigEndian.Uint32(buffer[8:12])

	response := make([]byte, 8)
	binary.BigEndian.PutUint16(response[0:4], 0)             // message_size
	binary.BigEndian.PutUint32(response[4:8], correlationId) // correlation_id
	_, err = conn.Write([]byte(response))
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		return
	}
}
