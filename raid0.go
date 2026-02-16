package main

import "sync"

type raid0Impl struct {
	array *RAIDArray
	mu    sync.RWMutex
}

func newRAID0(array *RAIDArray) *raid0Impl {
	return &raid0Impl{array: array}
}

func (r *raid0Impl) writeBlock(logicalBlockID int, data []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	diskIndex := logicalBlockID % r.array.numDisks
	physicalBlockID := logicalBlockID / r.array.numDisks

	return r.array.disks[diskIndex].WriteBlock(physicalBlockID, data)
}

func (r *raid0Impl) readBlock(logicalBlockID int) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	diskIndex := logicalBlockID % r.array.numDisks
	physicalBlockID := logicalBlockID / r.array.numDisks

	return r.array.disks[diskIndex].ReadBlock(physicalBlockID)
}
