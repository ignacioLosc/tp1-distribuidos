package protocol

import (
	"encoding/binary"
)


type PlatformCount struct {
	Windows uint32 
	Linux uint32
	Mac uint32 
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

func DeserializeCounter(bytes []byte) PlatformCount{
	windows := binary.BigEndian.Uint32(bytes[:4])
	linux := binary.BigEndian.Uint32(bytes[4:8])
	mac := binary.BigEndian.Uint32(bytes[8:12])

	return PlatformCount{windows, linux, mac}
}

func (c *PlatformCount) Increment (windowsCompatible bool, linuxCompatible bool, macCompatible bool) {
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

