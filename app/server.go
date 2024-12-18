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

type Message struct {
	messageSize   uint32
	apiKey        uint16
	apiVersion    uint16
	correlationId uint32
}

func read(conn net.Conn) (Message, error) {
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return Message{}, err
	}
	message := Message{
		messageSize:   binary.BigEndian.Uint32(buffer[0:4]),
		apiKey:        binary.BigEndian.Uint16(buffer[4:6]),
		apiVersion:    binary.BigEndian.Uint16(buffer[6:8]),
		correlationId: binary.BigEndian.Uint32(buffer[8:12]),
	}
	return message, nil
}

func send(conn net.Conn, message *[]byte) error {
	binary.Write(conn, binary.BigEndian, int32(len(*message)))
	binary.Write(conn, binary.BigEndian, message)
	return nil
}

func handleConnection(conn net.Conn) {
	for {
		buffer := make([]byte, 1024)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		message, err := read(conn)
		if err != nil {
			fmt.Println("Error reading message: ", err.Error())
			return
		}

		errorCode := 0
		if message.apiVersion > 4 {
			errorCode = 35
		}

		if message.apiKey == 18 {
			fmt.Println("ApiVersions request")
			response := make([]byte, 19)
			binary.BigEndian.PutUint32(response[0:], message.correlationId)
			binary.BigEndian.PutUint16(response[4:], uint16(errorCode))
			response[6] = 2
			// api version entry
			binary.BigEndian.PutUint16(response[7:], 18)
			binary.BigEndian.PutUint16(response[9:], 3)
			binary.BigEndian.PutUint16(response[11:], 4)
			response[13] = 0
			binary.BigEndian.PutUint32(response[14:], 0)
			response[18] = 0
			err = send(conn, &response)
			if err != nil {
				fmt.Println("Error sending response: ", err.Error())
				return
			}
		}
	}
}
