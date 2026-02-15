package main

import (
	"fmt"
	"os"
	"sync"
)

type Disk struct {
	file *os.File
	path string

	blockSize int
	numBlocks int

	failed bool

	mu sync.RWMutex

	writeCount uint64
	readCount  uint64
}

type DiskStats struct {
	Path       string
	WriteCount uint64
	ReadCount  uint64
	Failed     bool
}

func NewDisk(path string, blockSize, numBlocks int) (*Disk, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("block size must be positive, got %d", blockSize)
	}
	if numBlocks <= 0 {
		return nil, fmt.Errorf("number of blocks must be positive, got %d", numBlocks)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open disk %s: %w", path, err)
	}

	requiredSize := int64(blockSize * numBlocks)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() < requiredSize {
		if err := file.Truncate(requiredSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to resize disk: %w", err)
		}
	}

	return &Disk{
		file:      file,
		path:      path,
		blockSize: blockSize,
		numBlocks: numBlocks,
		failed:    false,
	}, nil
}

func (d *Disk) ReadBlock(blockID int) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.failed {
		return nil, fmt.Errorf("disk %s is failed", d.path)
	}

	if blockID < 0 || blockID >= d.numBlocks {
		return nil, fmt.Errorf("block ID %d out of bounds [0, %d)", blockID, d.numBlocks)
	}

	data := make([]byte, d.blockSize)
	offset := int64(blockID * d.blockSize)

	n, err := d.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("read error on %s block %d: %w", d.path, blockID, err)
	}
	if n != d.blockSize {
		return nil, fmt.Errorf("short read on %s: expected %d bytes, got %d", d.path, d.blockSize, n)
	}

	d.mu.RUnlock()
	d.mu.Lock()
	d.readCount++
	d.mu.Unlock()
	d.mu.RLock()

	return data, nil
}

func (d *Disk) WriteBlock(blockID int, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.failed {
		return fmt.Errorf("disk %s is failed", d.path)
	}

	if blockID < 0 || blockID >= d.numBlocks {
		return fmt.Errorf("block ID %d out of bounds [0, %d)", blockID, d.numBlocks)
	}

	if len(data) != d.blockSize {
		return fmt.Errorf("data size %d does not match block size %d", len(data), d.blockSize)
	}

	offset := int64(blockID * d.blockSize)
	n, err := d.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("write error on %s block %d: %w", d.path, blockID, err)
	}
	if n != d.blockSize {
		return fmt.Errorf("short write on %s: expected %d bytes, wrote %d", d.path, d.blockSize, n)
	}

	if err := d.file.Sync(); err != nil {
		return fmt.Errorf("sync error on %s: %w", d.path, err)
	}

	d.writeCount++

	return nil
}

func (d *Disk) SetFailed(failed bool) { // simulates hardware failure
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failed = failed
}

func (d *Disk) IsFailed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.failed
}

func (d *Disk) GetStats() DiskStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return DiskStats{
		Path:       d.path,
		WriteCount: d.writeCount,
		ReadCount:  d.readCount,
		Failed:     d.failed,
	}
}

func (d *Disk) Capacity() int {
	return d.numBlocks
}

func (d *Disk) BlockSize() int {
	return d.blockSize
}

func (d *Disk) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}
