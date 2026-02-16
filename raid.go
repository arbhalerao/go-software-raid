package main

import (
	"fmt"
	"sync"
)

type RAIDLevel int

const (
	RAID0 RAIDLevel = 0 // striping
	RAID1 RAIDLevel = 1 // mirroring
	RAID5 RAIDLevel = 5 // striping + distributed parity
)

type RAIDArray struct {
	level     RAIDLevel
	disks     []*Disk
	blockSize int
	numDisks  int
	capacity  int // total logical blocks
	mu        sync.RWMutex

	raid0 *raid0Impl
	raid1 *raid1Impl
	raid5 *raid5Impl
}

type RAIDConfig struct {
	Level         RAIDLevel
	DiskPaths     []string
	BlockSize     int
	BlocksPerDisk int
}

func NewRAIDArray(config RAIDConfig) (*RAIDArray, error) {
	if len(config.DiskPaths) < 2 {
		return nil, fmt.Errorf("RAID requires at least 2 disks")
	}

	if config.Level == RAID5 && len(config.DiskPaths) < 3 {
		return nil, fmt.Errorf("RAID 5 requires at least 3 disks")
	}

	if config.BlockSize <= 0 {
		return nil, fmt.Errorf("block size must be positive")
	}

	if config.BlocksPerDisk <= 0 {
		return nil, fmt.Errorf("blocks per disk must be positive")
	}

	disks := make([]*Disk, len(config.DiskPaths))
	for i, path := range config.DiskPaths {
		disk, err := NewDisk(path, config.BlockSize, config.BlocksPerDisk)
		if err != nil {
			for j := 0; j < i; j++ {
				disks[j].Close()
			}
			return nil, fmt.Errorf("failed to create disk %d: %w", i, err)
		}
		disks[i] = disk
	}

	r := &RAIDArray{
		level:     config.Level,
		disks:     disks,
		blockSize: config.BlockSize,
		numDisks:  len(disks),
	}

	switch config.Level {
	case RAID0:
		r.capacity = config.BlocksPerDisk * len(disks)
		r.raid0 = newRAID0(r)
	case RAID1:
		r.capacity = config.BlocksPerDisk
		r.raid1 = newRAID1(r)
	case RAID5:
		r.capacity = config.BlocksPerDisk * (len(disks) - 1)
		r.raid5 = newRAID5(r)
	default:
		r.Close()
		return nil, fmt.Errorf("unsupported RAID level: %d", config.Level)
	}

	return r, nil
}

func (r *RAIDArray) Capacity() int {
	return r.capacity
}

func (r *RAIDArray) Level() RAIDLevel {
	return r.level
}

func (r *RAIDArray) WriteBlock(logicalBlockID int, data []byte) error {
	if logicalBlockID < 0 || logicalBlockID >= r.capacity {
		return fmt.Errorf("logical block %d out of bounds [0, %d)", logicalBlockID, r.capacity)
	}

	if len(data) != r.blockSize {
		return fmt.Errorf("data size must match block size %d", r.blockSize)
	}

	switch r.level {
	case RAID0:
		return r.raid0.writeBlock(logicalBlockID, data)
	case RAID1:
		return r.raid1.writeBlock(logicalBlockID, data)
	case RAID5:
		return r.raid5.writeBlock(logicalBlockID, data)
	default:
		return fmt.Errorf("unsupported RAID level: %d", r.level)
	}
}

func (r *RAIDArray) ReadBlock(logicalBlockID int) ([]byte, error) {
	if logicalBlockID < 0 || logicalBlockID >= r.capacity {
		return nil, fmt.Errorf("logical block %d out of bounds [0, %d)", logicalBlockID, r.capacity)
	}

	switch r.level {
	case RAID0:
		return r.raid0.readBlock(logicalBlockID)
	case RAID1:
		return r.raid1.readBlock(logicalBlockID)
	case RAID5:
		return r.raid5.readBlock(logicalBlockID)
	default:
		return nil, fmt.Errorf("unsupported RAID level: %d", r.level)
	}
}

func (r *RAIDArray) RebuildDisk(diskIndex int) error { // rebuilds a failed disk (RAID 5 only)
	if r.level != RAID5 {
		return fmt.Errorf("disk rebuild only supported for RAID 5")
	}
	return r.raid5.rebuildDisk(diskIndex)
}

func (r *RAIDArray) GetStats() []DiskStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make([]DiskStats, len(r.disks))
	for i, disk := range r.disks {
		stats[i] = disk.GetStats()
	}
	return stats
}

func (r *RAIDArray) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstError error
	for i, disk := range r.disks {
		if err := disk.Close(); err != nil && firstError == nil {
			firstError = fmt.Errorf("failed to close disk %d: %w", i, err)
		}
	}
	return firstError
}
