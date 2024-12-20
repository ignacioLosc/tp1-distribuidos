package common

import (
	"fmt"
	"net"

	"example.com/system/communication/middleware"
	"example.com/system/communication/protocol"
	"example.com/system/communication/utils"
)

func (s *Server) waitForResults(conn net.Conn, clientId string) error {
	msgChan := make(chan middleware.MsgResponse)

	s.middleware.DeclareDirectQueue(communication, "query_results")
	qName, err := s.middleware.DeclareTemporaryQueue(communication)
	if err != nil {
		return fmt.Errorf("failed to declare temporary queue: %w.", err)
	}
	s.middleware.BindQueueToExchange(communication, "results", qName, clientId+".*")

	go s.middleware.ConsumeExchange(communication, qName, msgChan)
	for {
		select {
		case <-s.middleware.Ctx.Done():
			log.Infof("waitForResults returning")
			return nil

		case result := <-msgChan:
			stringResult := ""
			msg := result.Msg.Body
			result.Msg.Ack(false)
			switch result.Msg.RoutingKey {
			case clientId+".query1":
				counter, err := protocol.DeserializeCounter(msg)
				if err != nil {
					return fmt.Errorf("failed to deserialize counter: %w.", err)
				}
				stringResult = fmt.Sprintf("QUERY 1 RESULTS: Windows: %d, Linux: %d, Mac: %d", counter.Windows, counter.Linux, counter.Mac)
				break
			case clientId+".query2":
				gameNames := getGameNames(msg)
				stringResult = fmt.Sprintf("QUERY 2 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case clientId+".query3":
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 3 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case clientId+".query4":
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 4 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			case clientId+".query5":
				gameNames := getGameReviewCountNames(msg)
				stringResult = fmt.Sprintf("QUERY 5 RESULTS: ")
				stringResult = formatGameNames(stringResult, gameNames)
				break
			default:
				log.Errorf("invalid routing key: %s", result.Msg.RoutingKey)
			}

			data, err := utils.SerializeString(stringResult)
			if err != nil {
				return fmt.Errorf("failed to serialize data: %w.", err)
			}

			err = utils.SendAll(conn, data)
			if err != nil {
				return fmt.Errorf("failed to write data: %w.", err)
			}
		}
	}
}
