package main

import (
	"fmt"
	"sync"
)

type raid1Impl struct {
	array *RAIDArray
	mu    sync.RWMutex
}

type writeResult struct {
	diskIndex int
	err       error
}

func newRAID1(array *RAIDArray) *raid1Impl {
	return &raid1Impl{array: array}
}

func (r *raid1Impl) writeBlock(logicalBlockID int, data []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var wg sync.WaitGroup
	resultChan := make(chan writeResult, r.array.numDisks)

	for i := 0; i < r.array.numDisks; i++ {
		wg.Add(1)
		go func(diskIndex int) {
			defer wg.Done()
			err := r.array.disks[diskIndex].WriteBlock(logicalBlockID, data)
			resultChan <- writeResult{diskIndex: diskIndex, err: err}
		}(i)
	}

	wg.Wait()
	close(resultChan)

	successCount := 0
	var lastErr error
	failedDisks := make([]int, 0)

	for result := range resultChan {
		if result.err == nil {
			successCount++
		} else {
			lastErr = result.err
			failedDisks = append(failedDisks, result.diskIndex)
		}
	}

	if successCount == 0 {
		return fmt.Errorf("all disks failed to write: %w", lastErr)
	}

	if successCount < r.array.numDisks {
		return fmt.Errorf("degraded write: %d/%d disks succeeded, failed disks: %v",
			successCount, r.array.numDisks, failedDisks)
	}

	return nil
}

func (r *raid1Impl) readBlock(logicalBlockID int) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var lastErr error
	for i := 0; i < r.array.numDisks; i++ {
		if r.array.disks[i].IsFailed() {
			continue
		}

		data, err := r.array.disks[i].ReadBlock(logicalBlockID)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to read from any disk: %w", lastErr)
}
