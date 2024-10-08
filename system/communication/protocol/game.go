package protocol

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type Game struct {
	AppID                  string
	Name                   string
	WindowsCompatible      bool
	MacCompatible          bool
	LinuxCompatible        bool
	Genres                 string
	AveragePlaytimeForever int
	ReleaseDate            string
}

func GameFromRecord(record []string) (Game, error) {
	if len(record) < 38 {
		return Game{}, fmt.Errorf("Record has less than 38 fields")
	}

	game := Game{
		AppID:       record[0],
		Name:        record[1],
		ReleaseDate: record[2],
		Genres:       record[37],
	}

	windows := record[17]
	mac := record[18]
	linux := record[19]

	game.WindowsCompatible = windows == "True"
	game.MacCompatible = mac == "True"
	game.LinuxCompatible = linux == "True"

	AveragePlaytimeForever, err := strconv.Atoi(record[29])
	if err != nil {
		return Game{}, fmt.Errorf("Error parsing record's averagePlaytimeForever: %v", record[28])
	}
	game.AveragePlaytimeForever = AveragePlaytimeForever

	return game, nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func SerializeGame(game *Game) []byte {
	bytes := make([]byte, 0)

	appIdLen := len(game.AppID)
	bytes = append(bytes, byte(appIdLen))
	bytes = append(bytes, []byte(game.AppID)...)

	gameNameLen := len(game.Name)
	bytes = append(bytes, byte(gameNameLen))
	bytes = append(bytes, []byte(game.Name)...)

	gameGenresLenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(gameGenresLenBuffer, uint64(len(game.Genres)))
	bytes = append(bytes, gameGenresLenBuffer...)
	bytes = append(bytes, []byte(game.Genres)...)

	gameReleaseDateLen := len(game.ReleaseDate)
	bytes = append(bytes, byte(gameReleaseDateLen))
	bytes = append(bytes, []byte(game.ReleaseDate)...)

	averagePlaytimeForeverBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(averagePlaytimeForeverBuffer, uint64(game.AveragePlaytimeForever))
	bytes = append(bytes, averagePlaytimeForeverBuffer...)

	bytes = append(bytes, boolToByte(game.WindowsCompatible))
	bytes = append(bytes, boolToByte(game.MacCompatible))
	bytes = append(bytes, boolToByte(game.LinuxCompatible))

	return bytes
}

func DeserializeGame(bytes []byte) (Game, error, int) {
	errorMessage := fmt.Errorf("Not enough bytes to deserialize game")
	if len(bytes) < 1 {
		return Game{}, errorMessage, 0
	}

	index := 0

	appIdLen := uint64(bytes[index])
	index++

	if len(bytes) < index+int(appIdLen) {
		return Game{}, errorMessage, 0
	}

	appId := string(bytes[index : index+int(appIdLen)])
	index += int(appIdLen)

	if len(bytes) < index+1 {
		return Game{}, errorMessage, 0
	}
	nameLen := uint64(bytes[index])
	index++

	if len(bytes) < index+int(nameLen) {
		return Game{}, errorMessage, 0
	}
	name := string(bytes[index : index+int(nameLen)])
	index += int(nameLen)

	if len(bytes) < index+8 {
		return Game{}, errorMessage, 0
	}
	genresLen := binary.BigEndian.Uint64(bytes[index:index+8])
	index += 8

	if len(bytes) < index+int(genresLen) {
		return Game{}, errorMessage, 0
	}
	genres := string(bytes[index:(index+int(genresLen))])
	index += int(genresLen)

	if len(bytes) < index+1 {
		return Game{}, errorMessage, 0
	}
	releaseDateLen := uint64(bytes[index])

	if len(bytes) < index+int(releaseDateLen) {
		return Game{}, errorMessage, 0
	}
	index++
	releaseDate := string(bytes[index : index+int(releaseDateLen)])
	index += int(releaseDateLen)

	if len(bytes) < index+11 {
		return Game{}, errorMessage, 0
	}
	averagePlaytimeForever := binary.BigEndian.Uint64(bytes[index : index+8])
	index += 8


	windowsCompatible := bytes[index] == 1
	index++
	macCompatible := bytes[index] == 1
	index++
	linuxCompatible := bytes[index] == 1
	index++

	return Game{
		AppID:                 appId,
		Name:                  name,
		Genres:                genres,
		ReleaseDate:           releaseDate,
		AveragePlaytimeForever: int(averagePlaytimeForever),
		WindowsCompatible:     windowsCompatible,
		MacCompatible:         macCompatible,
		LinuxCompatible:       linuxCompatible,
	}, nil, index
}
