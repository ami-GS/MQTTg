package MQTTg

import (
	"encoding/binary"
	"fmt"
	"net"
)

func UTF8_encode(dst []uint8, s string) int {
	binary.BigEndian.PutUint16(dst, uint16(len(s)))
	copy(dst[2:], []uint8(s))
	return 2 + len(s)
}

func UTF8_decode(wire []uint8) (int, string) {
	length := int(binary.BigEndian.Uint16(wire) + 2)
	return length, string(wire[2:length])
}

func RemainEncode(dst []uint8, length uint32) int {
	i := 0
	for ; length > 0; i++ {
		digit := uint8(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		dst[i] = digit
	}
	return i
}

func RemainDecode(wire []byte) (uint32, int, error) {
	m := uint32(1)
	out := uint32(0)
	i := 0
	for ; ; i++ {
		out += uint32(wire[i]&0x7f) * m
		m *= 0x80

		if wire[i]&0x80 == 0 {
			break
		}
		if m > 2097152 {
			return 0, 0, MALFORMED_REMAIN_LENGTH
			//TODO:error handling
		}
	}
	return out, i + 1, nil
}

func GetLocalAddr() (*net.TCPAddr, error) {
	addrs, err := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				// MQTT default, TODO: set by config file
				return net.ResolveTCPAddr("tcp4", ipnet.IP.String()+":8883")
			}
		}
	}
	return nil, err
}

type MQTT_ERROR uint8 // for 256 errors

const (
	MALFORMED_REMAIN_LENGTH MQTT_ERROR = iota
	MALFORMED_SUBSCRIBE_RESERVED_PART
	MALFORMED_CONNECT_FLAG_BIT
	MALFORMED_FIXED_HEADER_RESERVED_BIT
	PROTOCOL_VIOLATION
	NOT_CONNECTED
	INVALID_MESSAGE_CAME
	INVALID_PROTOCOL_LEVEL
	INVALID_PROTOCOL_NAME
	CLIENT_NOT_EXIST
	CLIENT_TIMED_OUT
	CLIENT_ID_IS_USED_ALREADY
	CLEANSESSION_MUST_BE_TRUE
	PACKET_ID_IS_USED_ALREADY
	PACKET_ID_SHOULD_BE_ZERO
	PACKET_ID_SHOULD_NOT_BE_ZERO
	USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
	INVALID_QOS_3
	MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
	WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
	PACKET_ID_DOES_NOT_EXIST
	WILDCARD_CHARACTERS_IN_PUBLISH
	FAIL_TO_SET_PACKET_ID
	UNSUBSCRIBE_TO_NON_SUBSCRIBE_TOPIC
)

func EmitError(e error) {
	// TODO: added logging here
	if e != nil {
		str := ""
		if _, t := e.(MQTT_ERROR); t {
			str += "MQTT_ERROR"
		} else {
			str += "NORMAL_ERROR"
		}
		fmt.Printf("[%s] %s\n", ClError.Apply(str), e.Error())
	}
}

func (e MQTT_ERROR) Error() string {
	return []string{
		"MALFORMED_REMAIN_LENGTH",
		"MALFORMED_SUBSCRIBE_RESERVED_PART",
		"MALFORMED_CONNECT_FLAG_BIT",
		"MALFORMED_FIXED_HEADER_RESERVED_BIT",
		"PROTOCOL_VIOLATION",
		"NOT_CONNECTED",
		"INVALID_MESSAGE_CAME",
		"INVALID_PROTOCOL_LEVEL",
		"INVALID_PROTOCOL_NAME",
		"CLIENT_NOT_EXIST",
		"CLIENT_TIMED_OUT",
		"CLIENT_ID_IS_USED_ALREADY",
		"CLEANSESSION_MUST_BE_TRUE",
		"PACKET_ID_IS_USED_ALREADY",
		"PACKET_ID_SHOULD_BE_ZERO",
		"PACKET_ID_SHOULD_NOT_BE_ZERO",
		"USERNAME_DOES_NOT_EXIST_WITH_PASSWORD",
		"INVALID_QOS_3",
		"MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL",
		"WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME",
		"PACKET_ID_DOES_NOT_EXIST",
		"WILDCARD_CHARACTERS_IN_PUBLISH",
		"FAIL_TO_SET_PACKET_ID",
		"UNSUBSCRIBE_TO_NON_SUBSCRIBE_TOPIC",
	}[e]
}
