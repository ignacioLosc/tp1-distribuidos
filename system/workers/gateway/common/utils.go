package common

import (
	"encoding/binary"

	"example.com/system/communication/protocol"
)

func getGameNames(msg []byte) []string {
	lenGames := binary.BigEndian.Uint64(msg[:8])

	games := make([]string, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGame(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		games = append(games, game.Name)
		index += j
	}
	return games
}

func getGameReviewCountNames(msg []byte) []string {
	lenGames := binary.BigEndian.Uint64(msg[:8])

	games := make([]string, 0)

	index := 8
	for i := 0; i < int(lenGames); i++ {
		game, err, j := protocol.DeserializeGameReviewCount(msg[index:])

		if err != nil {
			log.Errorf("Failed to deserialize game", err)
			continue
		}

		games = append(games, game.AppName)
		index += j
	}
	return games
}

func formatGameNames(stringResult string, gameNames []string) string {
	for idx, gameName := range gameNames {
		if idx > 0 {
			stringResult += ", " + gameName
		} else {
			stringResult += gameName
		}
	}
	return stringResult
}

