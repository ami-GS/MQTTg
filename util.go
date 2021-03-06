package MQTTg

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func UTF8_encode(w io.Writer, s string) int {
	binary.Write(w, binary.BigEndian, uint16(len(s)))
	_, _ = w.Write([]byte(s))
	return 2 + len(s)
}

func UTF8_decode(r io.Reader, str *string) (len uint16) {
	binary.Read(r, binary.BigEndian, &len)
	data := make([]byte, len)
	binary.Read(r, binary.BigEndian, &data)
	*str = string(data)
	len += 2
	return len
}

func RemainEncode(w io.Writer, length uint32) int {
	i := 0
	if length == 0 {
		binary.Write(w, binary.BigEndian, uint8(0))
		return i
	}

	for ; length > 0; i++ {
		digit := uint8(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		binary.Write(w, binary.BigEndian, &digit)
	}
	return i
}

func RemainDecode(r io.Reader, remLen *uint32) (int, error) {
	m := uint32(1)
	i := 0
	var tmp byte
	for ; ; i++ {
		binary.Read(r, binary.BigEndian, &tmp)
		*remLen += uint32(tmp&0x7f) * m
		m *= 0x80

		if tmp&0x80 == 0 {
			break
		}
		if m > 2097152 {
			return 0, MALFORMED_REMAIN_LENGTH
			//TODO:error handling
		}
	}
	return i + 1, nil
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
	SERVER_TIMED_OUT
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
		"SREVER_TIMED_OUT",
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
