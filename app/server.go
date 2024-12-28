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

	registery := NewRequestRegistry()
	registery.RegisterHandler(18, NewApiVersionsHandler())
	registery.RegisterHandler(75, NewDescribeTopicPartsHandler())

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Did accept connection")
		go handleConnection(registery, conn)
	}
}

type Message struct {
	messageSize   uint32
	apiKey        uint16
	apiVersion    uint16
	correlationId uint32
	clientId      string
	requestBody   []byte
}

func read(conn net.Conn) (Message, error) {
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return Message{}, err
	}
	messageSize := binary.BigEndian.Uint32(buffer[0:4])
	rawMessage := buffer[4:messageSize]
	message := Message{}
	message.messageSize = messageSize
	message.apiKey = binary.BigEndian.Uint16(rawMessage[0:])
	message.apiVersion = binary.BigEndian.Uint16(rawMessage[2:])
	message.correlationId = binary.BigEndian.Uint32(rawMessage[4:])

	clientIdLength := binary.BigEndian.Uint16(rawMessage[8:])
	if clientIdLength > 0 {
		message.clientId = string(rawMessage[10 : 10+clientIdLength])
	}
	rawMessage = rawMessage[10+clientIdLength:]
	// FIXME: assuming an empty tag buffer, below
	fmt.Println("message", message)
	message.requestBody = rawMessage[1:]
	return message, nil
}

func send(conn net.Conn, message *[]byte) error {
	binary.Write(conn, binary.BigEndian, int32(len(*message)))
	binary.Write(conn, binary.BigEndian, message)
	return nil
}

func handleConnection(handlerRegistry *RequestRegistry, conn net.Conn) {
	for {
		message, err := read(conn)
		if err != nil {
			fmt.Println("Error reading message: ", err.Error())
			return
		}

		// errorCode := 0
		// if message.apiVersion > 4 {
		// 	errorCode = 35
		// }
		handler, err := handlerRegistry.GetHandler(message.apiKey)
		if err != nil {
			fmt.Println("Error getting handler: ", err.Error())
			return
		}
		err = handler.Parse(message.requestBody)
		if err != nil {
			fmt.Println("Error parsing request: ", err.Error())
			return
		}
		response, err := handler.Handle(message)
		if err != nil {
			fmt.Println("Error handling request: ", err.Error())
			return
		}
		err = send(conn, &response)
		if err != nil {
			fmt.Println("Error sending response: ", err.Error())
			return
		}
	}
}
