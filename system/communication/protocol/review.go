package protocol

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type Review struct {
	AppID       string
	AppName     string
	ReviewText  string
	ReviewScore int8
	ReviewVotes int
}

type MappedReview struct {
	AppID             string
	IsPositive        bool
	IsNegative        bool
	IsPositiveEnglish bool
}

func ReviewFromRecord(record []string) (Review, error) {
	reviewScore, err1 := strconv.Atoi(record[3])
	reviewVotes, err2 := strconv.Atoi(record[4])

	if err1 != nil || err2 != nil {
		return Review{}, fmt.Errorf("Error parsing record: %v %v", record[3], record[4])
	}

	return Review{
		AppID:       record[0],
		AppName:     record[1],
		ReviewText:  record[2],
		ReviewScore: int8(reviewScore),
		ReviewVotes: reviewVotes,
	}, nil
}

func SerializeReview(review *Review) []byte {
	bytes := make([]byte, 0)

	appIdLen := len(review.AppID)
	bytes = append(bytes, byte(appIdLen))
	bytes = append(bytes, []byte(review.AppID)...)

	bytes = append(bytes, byte(review.ReviewScore))

	reviewTextLenBuffer := make([]byte, 8)
	binary.BigEndian.PutUint64(reviewTextLenBuffer, uint64(len(review.ReviewText)))
	bytes = append(bytes, reviewTextLenBuffer...)

	bytes = append(bytes, []byte(review.ReviewText)...)

	return bytes
}

func SerializeMappedReview(review *MappedReview) []byte {
	bytes := make([]byte, 0)

	appIdLen := len(review.AppID)
	bytes = append(bytes, byte(appIdLen))
	bytes = append(bytes, []byte(review.AppID)...)

	bytes = append(bytes, boolToByte(review.IsPositive))
	bytes = append(bytes, boolToByte(review.IsNegative))
	bytes = append(bytes, boolToByte(review.IsPositiveEnglish))

	return bytes
}

func DeserializeMappedReview(bytes []byte) (MappedReview, error, int) {
	index := 0
	errorMessage := fmt.Errorf("Not enough bytes to deserialize mapped review")
	if len(bytes) < 1 {
		return MappedReview{}, errorMessage, 0
	}

	appIdLen := uint64(bytes[index])
	index++

	if len(bytes) < int(appIdLen)+10 {
		return MappedReview{}, errorMessage, 0
	}

	appId := string(bytes[index : appIdLen+1])
	index += int(appIdLen) + 1

	isPositive := bytes[index] == 1
	index++
	isNegative := bytes[index] == 1
	index++
	isPositiveEnglish := bytes[index] == 1
	index++

	return MappedReview{
		AppID:             appId,
		IsPositive:        isPositive,
		IsNegative:        isNegative,
		IsPositiveEnglish: isPositiveEnglish,
	}, nil, index
}

func DeserializeReview(bytes []byte) (Review, error, int) {
	index := 0
	errorMessage := fmt.Errorf("Not enough bytes to deserialize review")
	if len(bytes) < 1 {
		return Review{}, errorMessage, 0
	}

	appIdLen := uint64(bytes[index])
	index++

	if len(bytes) < int(appIdLen)+10 {
		return Review{}, errorMessage, 0
	}

	appId := string(bytes[index : appIdLen+1])
	index += int(appIdLen) + 1

	reviewScore := int8(bytes[index])
	index++

	reviewTextLen := binary.BigEndian.Uint64(bytes[index : index+8])
	index += 8

	if len(bytes) < index+int(reviewTextLen) {
		return Review{}, errorMessage, 0
	}

	reviewText := string(bytes[index : index+int(reviewTextLen)])
	index += int(reviewTextLen)

	return Review{
		AppID:       appId,
		ReviewScore: reviewScore,
		ReviewText:  reviewText,
	}, nil, index
}
