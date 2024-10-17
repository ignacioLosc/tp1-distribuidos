package protocol

import (
	"encoding/binary"
	"fmt"
)

type PlatformCount struct {
	Windows uint32
	Linux   uint32
	Mac     uint32
}

func (c *PlatformCount) Restart() {
	c.Windows = 0
	c.Linux = 0
	c.Mac = 0
	return 
}

func (c *PlatformCount) Serialize() []byte {
	bytes := make([]byte, 0)

	windows := make([]byte, 4)
	binary.BigEndian.PutUint32(windows, c.Windows)
	bytes = append(bytes, windows...)

	linux := make([]byte, 4)
	binary.BigEndian.PutUint32(linux, c.Linux)
	bytes = append(bytes, linux...)

	mac := make([]byte, 4)
	binary.BigEndian.PutUint32(mac, c.Mac)
	bytes = append(bytes, mac...)

	return bytes
}

func DeserializeCounter(bytes []byte) (PlatformCount, error) {
	if len(bytes) != 12 {
		return PlatformCount{}, fmt.Errorf("invalid counter data")
	}

	windows := binary.BigEndian.Uint32(bytes[:4])
	linux := binary.BigEndian.Uint32(bytes[4:8])
	mac := binary.BigEndian.Uint32(bytes[8:12])

	return PlatformCount{windows, linux, mac}, nil
}

func (c *PlatformCount) Increment(windowsCompatible bool, linuxCompatible bool, macCompatible bool) {
	if windowsCompatible {
		c.Windows++
	}
	if linuxCompatible {
		c.Linux++
	}
	if macCompatible {
		c.Mac++
	}
}

func (c *PlatformCount) IncrementVals(windowsCompatible uint32, linuxCompatible uint32, macCompatible uint32) {
	c.Windows += windowsCompatible
	c.Linux += linuxCompatible
	c.Mac += macCompatible
}
