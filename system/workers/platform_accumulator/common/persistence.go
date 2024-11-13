package common

import (
	"encoding/gob"
	"fmt"
	"os"

	prot "example.com/system/communication/protocol"
)

type PlatformAccumulatorExport struct {
	CountMap    map[string]prot.PlatformCount
	FinishedMap map[string]int
}

func saveToFile(filename string, pa *PlatformAccumulator) error {
	exportData := PlatformAccumulatorExport{
		CountMap:    pa.countMap,
		FinishedMap: pa.finishedMap,
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

func readFromFile(filename string) (PlatformAccumulator, error) {

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return PlatformAccumulator{
				countMap:   make(map[string]prot.PlatformCount),
				finishedMap:  make(map[string]int),
		}, nil
	}

	var pa PlatformAccumulator
	var exportData PlatformAccumulatorExport

	file, err := os.Open(filename)
	if err != nil {
		return pa, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&exportData); err != nil {
		return pa, fmt.Errorf("failed to decode data: %w", err)
	}

	pa.countMap = exportData.CountMap
	pa.finishedMap = exportData.FinishedMap

	return pa, nil
}
