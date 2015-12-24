package MQTTg

import (
	"encoding/binary"
	"fmt"
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
	CLIENT_ID_IS_USED_ALREADY
	USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
	INVALID_QOS_3
	MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL
	WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME
	PACKET_ID_DOES_NOT_EXIST
)

func EmitError(e error) {
	// TODO: added logging here
	fmt.Println(e.Error)
}

func (e MQTT_ERROR) Error() string {
	er_st := []string{
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
		"CLIENT_ID_IS_USED_ALREADY",
		"USERNAME_DOES_NOT_EXIST_WITH_PASSWORD",
		"INVALID_QOS_3",
		"MULTI_LEVEL_WILDCARD_MUST_BE_ON_TAIL",
		"WILDCARD_MUST_NOT_BE_ADJACENT_TO_NAME",
		"PACKET_ID_DOES_NOT_EXIST",
	}
	return er_st[e]
}
