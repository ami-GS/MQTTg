package MQTTg

import (
	"encoding/binary"
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

func RemainingLength(length uint32) (out []uint8) {
	return
}
