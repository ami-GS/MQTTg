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

func RemainDecode(wire []byte) (uint32, error) {
	multiplier := uint32(1)
	out := uint32(wire[0] & 0x7f)
	for idx := 1; wire[idx]&0x80 == 0x80; idx++ {
		out += uint32(wire[idx]&0x7f) * multiplier
		multiplier *= 0x80
		if multiplier > 2097152 {
			return 0, MALFORMED_REMAIN_LENGTH
			//TODO:error handling
		}
	}
	return out, nil
}

type MQTT_ERROR uint8 // for 256 errors

const (
	MALFORMED_REMAIN_LENGTH MQTT_ERROR = iota
	NOT_CONNECTED
	INVALID_MESSAGE_CAME
	INVALID_PROTOCOL_LEVEL
	CLIENT_NOT_EXIST
	CLIENT_ID_IS_USED_ALREADY
	USERNAME_DOES_NOT_EXIST_WITH_PASSWORD
)

func EmitError(e error) {
	// TODO: added logging here
	fmt.Println(e.Error)
}

func (e MQTT_ERROR) Error() string {
	er_st := []string{
		"MALFORMED_REMAIN_LENGTH",
		"NOT_CONNECTED",
		"INVALID_MESSAGE_CAME",
		"INVALID_PROTOCOL_LEVEL",
		"CLIENT_NOT_EXIST",
		"CLIENT_ID_IS_USED_ALREADY",
		"USERNAME_DOES_NOT_EXIST_WITH_PASSWORD",
	}
	return er_st[e]
}
