package common

import (
	"encoding/gob"
	"fmt"
	"os"

	prot "example.com/system/communication/protocol"
)

type PlatformCounterExport struct {
	CountMap       map[string]prot.PlatformCount
	FinishedMap    map[string]bool
	EndOfGamesQueue string
}

func saveToFile(filename string, pc *PlatformCounter) error {
	exportData := PlatformCounterExport{
		CountMap:       pc.countMap,
		FinishedMap:    pc.finishedMap,
		EndOfGamesQueue: pc.endOfGamesQueue,
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(exportData); err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}
	return nil
}

func readFromFile(filename string) (PlatformCounter, error) {
	var pc PlatformCounter 
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return PlatformCounter{
			countMap:    make(map[string]prot.PlatformCount),
			finishedMap: make(map[string]bool),
			endOfGamesQueue: "",
		}, nil
	}

	var exportData PlatformCounterExport

	file, err := os.Open(filename)
	if err != nil {
		return pc, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&exportData); err != nil {
		return pc, fmt.Errorf("failed to decode data: %w", err)
	}

	pc.countMap = exportData.CountMap
	pc.finishedMap = exportData.FinishedMap
	pc.endOfGamesQueue = exportData.EndOfGamesQueue

	return pc, nil
}
