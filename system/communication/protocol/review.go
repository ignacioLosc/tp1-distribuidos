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

func ReviewFromRecord(record []string) (Review, error) {
	reviewScore, err1 := strconv.Atoi(record[3])
	reviewVotes, err2 := strconv.Atoi(record[4])

	if err1 != nil || err2 != nil {
		return Review{}, fmt.Errorf("Error parsing record: %v %v", record[3], record[4])
	}

	return Review {
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

func DeserializeReview(bytes []byte) (Review, error) {
	appIdLen := uint64(bytes[0])
	appId := string(bytes[1:appIdLen+1])

	reviewScore := int8(bytes[appIdLen+1])

	reviewTextLen := binary.BigEndian.Uint64(bytes[appIdLen+2:appIdLen+10])
	reviewText := string(bytes[appIdLen+10:appIdLen+10+reviewTextLen])

	return Review {
		AppID:       appId,
		ReviewScore: reviewScore,
		ReviewText:  reviewText,
	}, nil
}
