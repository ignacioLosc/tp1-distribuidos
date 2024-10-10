package protocol

import (
	"encoding/binary"
	"fmt"
)

type GameReviewCount struct {
	AppName                    string
	PositiveReviewCount        int
	NegativeReviewCount        int
	PositiveEnglishReviewCount int
}

func SerializeGameReviewCount(gameReviewCount *GameReviewCount) []byte {
	bytes := make([]byte, 0)

	appNameLen := len(gameReviewCount.AppName)
	bytes = append(bytes, byte(appNameLen))
	bytes = append(bytes, []byte(gameReviewCount.AppName)...)

	positiveReviewCountBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(positiveReviewCountBuffer, uint64(gameReviewCount.PositiveReviewCount))
	bytes = append(bytes, positiveReviewCountBuffer...)

	negativeReviewCountBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(negativeReviewCountBuffer, uint64(gameReviewCount.NegativeReviewCount))
	bytes = append(bytes, negativeReviewCountBuffer...)

	positiveEnglishReviewCountBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(positiveEnglishReviewCountBuffer, uint64(gameReviewCount.PositiveEnglishReviewCount))
	bytes = append(bytes, positiveEnglishReviewCountBuffer...)

	return bytes
}

func DeserializeGameReviewCount(bytes []byte) (GameReviewCount, error, int) {
	index := 0
	errorMessage := fmt.Errorf("Not enough bytes to deserialize game review count")
	if len(bytes) < 1 {
		return GameReviewCount{}, errorMessage, 0
	}

	appNameLen := uint64(bytes[index])
	index++

	if len(bytes) < index+int(appNameLen)+24 {
		return GameReviewCount{}, errorMessage, 0
	}

	appName := string(bytes[index : index+int(appNameLen)])
	index += int(appNameLen)

	positiveReviewCount := binary.BigEndian.Uint64(bytes[index : index+8])
	index += 8

	negativeReviewCount := binary.BigEndian.Uint64(bytes[index : index+8])
	index += 8

	positiveEnglishReviewCount := binary.BigEndian.Uint64(bytes[index : index+8])
	index += 8

	return GameReviewCount{
		AppName:                    appName,
		PositiveReviewCount:        int(positiveReviewCount),
		NegativeReviewCount:        int(negativeReviewCount),
		PositiveEnglishReviewCount: int(positiveEnglishReviewCount),
	}, nil, index
}
