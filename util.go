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

func RemainDecode(wire []byte) (out uint32) {
	multiplier := uint32(1)
	out = uint32(wire[0] & 0x7f)
	for idx := 1; wire[idx]&0x80 == 0x80; idx++ {
		out += uint32(wire[idx]&0x7f) * multiplier
		multiplier *= 0x80
		//if multiplier > 0x80*0x80*0x80 {
		// TODO:error handling
		//}
	}
	return
}
