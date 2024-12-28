package main

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Skip first 3 bits, so start shifting from position 3
	READ             = 1 << 3 // 0000 0000 1000
	WRITE            = 1 << 4 // 0000 0001 0000
	CREATE           = 1 << 5 // 0000 0010 0000
	DELETE           = 1 << 6 // 0000 0100 0000
	ALTER            = 1 << 7 // 0001 0000 0000
	DESCRIBE         = 1 << 8 // 0000 1000 0000
	DESCRIBE_CONFIGS = 1 << 10
	ALTER_CONFIGS    = 1 << 11
	LIST             = 1 << 9 // 0010 0000 0000

)

type RequestHandler interface {
	Parse(body []byte) error
	Handle(m Message) ([]byte, error)
}

type DescribeTopicPartsRequest struct {
	Topics                 []string
	ResponsePartitionLimit uint32
	Cursor                 *string
}

type DescribeTopicPartsHandler struct {
	request DescribeTopicPartsRequest
}

func NewDescribeTopicPartsHandler() *DescribeTopicPartsHandler {
	return &DescribeTopicPartsHandler{}
}

func (h *DescribeTopicPartsHandler) Parse(body []byte) error {
	if len(body) < 2 {
		return errors.New("DescribeTopicPartsRequest request body too short")
	}
	topicCount := int8(body[0])
	fmt.Println("topicCount", topicCount)
	offset := 1
	topics := make([]string, 0, topicCount)

	for range int(topicCount) - 1 {
		if offset >= len(body) {
			return errors.New("DescribeTopicPartsRequest invalid body length while parsing topics")
		}
		topicLen := int8(body[offset])
		fmt.Println("topicLen", topicLen)

		if offset+int(topicLen) > len(body) {
			return errors.New("DescribeTopicPartsRequest invalid topic length")
		}
		topics = append(topics, string(body[offset:offset+int(topicLen)]))
		offset += int(topicLen) - 1
	}
	fmt.Println("topics", topics)
	fmt.Println("offset, body", offset, len(body))
	if offset+4 > len(body) {
		return errors.New("DescribeTopicPartsRequest invalid body length while parsing response partition limit")
	}
	partitionLimit := binary.BigEndian.Uint32(body[offset : offset+4])
	offset += 4
	fmt.Println("partitionLimit", partitionLimit, "offset", offset)
	if offset >= len(body) {
		return errors.New("DescribeTopicPartsRequest invalid body length while parsing cursor")
	}
	cursorLen := int8(body[offset])
	offset++
	fmt.Println("topics", topics)

	var cursor *string
	if cursorLen != -1 {
		if offset+int(cursorLen) > len(body) {
			return errors.New("DescribeTopicPartsRequest invalid cursor length")
		}
		cursorStr := string(body[offset : offset+int(cursorLen)])
		cursor = &cursorStr
	}
	h.request = DescribeTopicPartsRequest{
		Topics:                 topics,
		ResponsePartitionLimit: partitionLimit,
		Cursor:                 cursor,
	}
	return nil
}

func (h *DescribeTopicPartsHandler) Handle(message Message) ([]byte, error) {
	response := make([]byte, 100) // TODO: calculate this
	// header
	binary.BigEndian.PutUint32(response[0:], message.correlationId)
	response[4] = 0 // empty tag buffer

	// response body
	binary.BigEndian.PutUint32(response[5:], 0)   // throttle time
	response[9] = byte(len(h.request.Topics) + 1) // topic array length

	offset := 10
	for _, topic := range h.request.Topics {
		binary.BigEndian.PutUint16(response[offset:], 3) // error code
		topicNameLen := byte(len(topic))
		response[offset+2] = topicNameLen // topic length
		copy(response[offset+3:], topic)  // topic name
		offset += 3 + int(topicNameLen)

		copy(response[offset:offset+64], "00000000-0000-0000-0000-000000000000") // partitions
		offset += 64
		response[offset] = 0 // is internal false
		offset++
		response[offset] = 1 // partition count (here it is empty)
		offset++
		binary.BigEndian.PutUint32(response[offset:], uint32(READ|WRITE|CREATE|DELETE|ALTER|DESCRIBE|DESCRIBE_CONFIGS|ALTER_CONFIGS)) // supported operations
		offset += 4
		response[offset] = 0 // tag buffer
	}
	response[offset] = 0xFF // next cursor
	offset++
	response[offset] = 0 // tag buffer

	return response, nil
}

type (
	ApiVersionsRequest struct{}
	ApiVersionsHandler struct{}
)

func NewApiVersionsHandler() *ApiVersionsHandler {
	return &ApiVersionsHandler{}
}

func (h *ApiVersionsHandler) Parse(body []byte) error {
	return nil
}

func (h *ApiVersionsHandler) Handle(message Message) ([]byte, error) {
	fmt.Println("ApiVersions request")
	response := make([]byte, 26)
	binary.BigEndian.PutUint32(response[0:], message.correlationId)
	binary.BigEndian.PutUint16(response[4:], uint16(0))
	response[6] = 3
	// api version entry
	binary.BigEndian.PutUint16(response[7:], 18)
	binary.BigEndian.PutUint16(response[9:], 0)
	binary.BigEndian.PutUint16(response[11:], 4)
	response[13] = 0 // tag buffer
	// describe topic partitions entry
	binary.BigEndian.PutUint16(response[14:], 75)
	binary.BigEndian.PutUint16(response[16:], 0)
	binary.BigEndian.PutUint16(response[18:], 0)
	response[19] = 0 // tag buffer
	binary.BigEndian.PutUint32(response[20:], 0)
	response[24] = 0 // throttle time
	response[25] = 0
	return response, nil
}

type RequestRegistry struct {
	handlers map[uint16]RequestHandler
}

func NewRequestRegistry() *RequestRegistry {
	return &RequestRegistry{
		handlers: make(map[uint16]RequestHandler),
	}
}

func (r *RequestRegistry) RegisterHandler(requestType uint16, handler RequestHandler) {
	r.handlers[requestType] = handler
}

func (r *RequestRegistry) GetHandler(requestType uint16) (RequestHandler, error) {
	handler, exists := r.handlers[requestType]
	if !exists {
		return nil, errors.New("unknown request type")
	}
	return handler, nil
}
