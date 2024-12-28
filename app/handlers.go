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
	offset := 1
	topics := make([]string, 0, topicCount)

	for range int(topicCount) - 1 {
		if offset >= len(body) {
			return errors.New("DescribeTopicPartsRequest invalid body length while parsing topics")
		}
		topicLen := int8(body[offset]) - 1
		offset++
		if offset+int(topicLen) > len(body) {
			return errors.New("DescribeTopicPartsRequest invalid topic length")
		}
		topics = append(topics, string(body[offset:offset+int(topicLen)]))
		offset += int(topicLen)
	}

	if offset+4 > len(body) {
		return errors.New("DescribeTopicPartsRequest invalid body length while parsing response partition limit")
	}
	partitionLimit := binary.BigEndian.Uint32(body[offset : offset+4])
	offset += 4

	var cursor *string
	if offset < len(body) {
		cursorLen := int8(body[offset])
		offset++
		if cursorLen != -1 {
			if offset+int(cursorLen) > len(body) {
				return errors.New("DescribeTopicPartsRequest invalid cursor length")
			}
			cursorStr := string(body[offset : offset+int(cursorLen)])
			cursor = &cursorStr
			offset += int(cursorLen)
		}
		// NOTE: ignoring the tag buffer
	}

	h.request = DescribeTopicPartsRequest{
		Topics:                 topics,
		ResponsePartitionLimit: partitionLimit,
		Cursor:                 cursor,
	}
	return nil
}

func (h *DescribeTopicPartsHandler) Handle(message Message) ([]byte, error) {
	fmt.Println("DescribeTopicPartsHandler.Handle")
	fmt.Println("topics", h.request.Topics)
	response := make([]byte, 200) // TODO: calculate this
	size := 4
	// header
	binary.BigEndian.PutUint32(response[size:], message.correlationId)
	size += 4
	response[size] = 0 // empty tag buffer
	size++

	// response body
	binary.BigEndian.PutUint32(response[size:], 0) // throttle time
	size += 4
	response[size] = 0x02 // topic array length
	size++

	for _, topic := range h.request.Topics {

		binary.BigEndian.PutUint16(response[size:], 3) // error code
		size += 2

		// topic
		response[size] = byte(len(topic) + 1) // topic length
		size++
		copy(response[size:], topic) // topic name
		size += len(topic)

		// topic ID
		binary.BigEndian.PutUint64(response[size:], 0)
		binary.BigEndian.PutUint64(response[size+8:], 0)
		size += 16

		response[size] = 0 // is internal false
		size++

		response[size] = 0 // partition count (here it is empty)
		size++

		binary.BigEndian.PutUint32(response[size:], READ|WRITE|CREATE|DELETE|ALTER|DESCRIBE|DESCRIBE_CONFIGS|ALTER_CONFIGS) // supported operations
		size += 4
		response[size] = 0 // tag buffer
		size++
	}

	response[size] = 0xff // next cursor
	size++
	response[size] = 0 // tag buffer
	size++

	binary.BigEndian.PutUint32(response[0:], uint32(size-4))
	return response[:size], nil
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
	fmt.Println("ApiVersionsHandler.Handle")
	response := make([]byte, 100)
	size := 4

	// header
	binary.BigEndian.PutUint32(response[size:], message.correlationId)
	size += 4
	if message.apiVersion > 4 {
		binary.BigEndian.PutUint16(response[size:], 35)
	} else {
		binary.BigEndian.PutUint16(response[size:], 0)
	}
	size += 2

	response[size] = 3
	size++

	// api version entry
	binary.BigEndian.PutUint16(response[size:], 18)
	size += 2
	binary.BigEndian.PutUint16(response[size:], 0)
	size += 2
	binary.BigEndian.PutUint16(response[size:], 4)
	size += 2

	response[size] = 0 // tag buffer
	size++
	// describe topic partitions entry
	binary.BigEndian.PutUint16(response[size:], 75)
	size += 2
	binary.BigEndian.PutUint16(response[size:], 0)
	size += 2
	binary.BigEndian.PutUint16(response[size:], 0)
	size += 2
	response[size] = 0 // tag buffer
	size++
	binary.BigEndian.PutUint32(response[size:], 0)
	size += 4
	response[size] = 0 // throttle time
	size++
	response[size] = 0

	binary.BigEndian.PutUint32(response[0:], uint32(size-4))
	return response[:size], nil
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
