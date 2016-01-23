package MQTTg

import (
	soac "github.com/ami-GS/soac/go"
	"math/rand"
	"time"
)

// This is for frame send/recv string debug
var FrameDebug bool = true

var ClSend, ClRecv, ClError *soac.Changer
var ClFrames []*soac.Changer

func init() {
	// This avoids packet ID from being same between client and broker
	rand.Seed(time.Now().UnixNano())
	ClSend, ClRecv, ClError = soac.NewChanger(), soac.NewChanger(), soac.NewChanger()
	ClSend.Cyan().Underline()
	ClRecv.Magenda().Underline()
	ClError.Red().Underline()

	ClFrames = make([]*soac.Changer, 16)
	for i := 0; i < 16; i++ {
		ClFrames[i] = soac.NewChanger()
	}
	ClFrames[Connect].Set256(27)
	ClFrames[Connack].Set256(207)
	ClFrames[Publish].Set256(148)
	ClFrames[Puback].Set256(142)
	ClFrames[Pubrec].Set256(136)
	ClFrames[Pubrel].Set256(130)
	ClFrames[Pubcomp].Set256(124)
	ClFrames[Subscribe].Set256(41)
	ClFrames[Suback].Set256(190)
	ClFrames[Unsubscribe].Set256(56)
	ClFrames[Unsuback].Set256(140)
	ClFrames[Pingreq].Set256(243)
	ClFrames[Pingresp].Set256(237)
	ClFrames[Disconnect].Set256(160)
}
