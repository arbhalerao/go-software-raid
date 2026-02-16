package main

import (
	"fmt"
	"sync"
)

type raid5Impl struct {
	array *RAIDArray
	mu    sync.Mutex
}

func newRAID5(array *RAIDArray) *raid5Impl {
	return &raid5Impl{array: array}
}

func (r *raid5Impl) writeBlock(logicalBlockID int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	stripeNum := logicalBlockID / (r.array.numDisks - 1)
	stripeOffset := logicalBlockID % (r.array.numDisks - 1)

	parityDisk := stripeNum % r.array.numDisks

	dataDisk := stripeOffset
	if dataDisk >= parityDisk {
		dataDisk++
	}

	parity := make([]byte, r.array.blockSize)
	copy(parity, data)

	for i := 0; i < r.array.numDisks-1; i++ {
		if i == stripeOffset {
			continue
		}

		diskIdx := i
		if diskIdx >= parityDisk {
			diskIdx++
		}

		if r.array.disks[diskIdx].IsFailed() {
			continue
		}

		blockData, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
		if err != nil {
			return fmt.Errorf("cannot calculate parity: failed to read disk %d: %w", diskIdx, err)
		}

		xorBytes(parity, blockData)
	}

	if !r.array.disks[parityDisk].IsFailed() {
		if err := r.array.disks[parityDisk].WriteBlock(stripeNum, parity); err != nil {
			return fmt.Errorf("failed to write parity to disk %d: %w", parityDisk, err)
		}
	}

	if err := r.array.disks[dataDisk].WriteBlock(stripeNum, data); err != nil {
		return fmt.Errorf("failed to write data to disk %d: %w", dataDisk, err)
	}

	return nil
}

func (r *raid5Impl) readBlock(logicalBlockID int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stripeNum := logicalBlockID / (r.array.numDisks - 1)
	stripeOffset := logicalBlockID % (r.array.numDisks - 1)

	parityDisk := stripeNum % r.array.numDisks

	dataDisk := stripeOffset
	if dataDisk >= parityDisk {
		dataDisk++
	}

	if !r.array.disks[dataDisk].IsFailed() {
		data, err := r.array.disks[dataDisk].ReadBlock(stripeNum)
		if err == nil {
			return data, nil
		}
	}

	fmt.Printf("  [RAID5] Degraded read: reconstructing block %d from parity\n", logicalBlockID)
	return r.reconstructBlock(stripeNum, dataDisk, parityDisk)
}

func (r *raid5Impl) reconstructBlock(stripeNum, missingDisk, parityDisk int) ([]byte, error) {
	if r.array.disks[parityDisk].IsFailed() {
		return nil, fmt.Errorf("cannot reconstruct: parity disk %d failed", parityDisk)
	}

	reconstructed, err := r.array.disks[parityDisk].ReadBlock(stripeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read parity from disk %d: %w", parityDisk, err)
	}

	for i := 0; i < r.array.numDisks; i++ {
		if i == parityDisk || i == missingDisk {
			continue
		}

		if r.array.disks[i].IsFailed() {
			return nil, fmt.Errorf("cannot reconstruct: multiple disk failures")
		}

		blockData, err := r.array.disks[i].ReadBlock(stripeNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read disk %d for reconstruction: %w", i, err)
		}

		xorBytes(reconstructed, blockData)
	}

	return reconstructed, nil
}

func (r *raid5Impl) rebuildDisk(diskIndex int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if diskIndex < 0 || diskIndex >= r.array.numDisks {
		return fmt.Errorf("invalid disk index %d", diskIndex)
	}

	if !r.array.disks[diskIndex].IsFailed() {
		return fmt.Errorf("disk %d is not marked as failed", diskIndex)
	}

	fmt.Printf("\n[REBUILD] Starting rebuild of disk %d...\n", diskIndex)

	r.array.disks[diskIndex].SetFailed(false)

	maxStripes := r.array.disks[diskIndex].Capacity()

	rebuiltBlocks := 0
	for stripeNum := 0; stripeNum < maxStripes; stripeNum++ {
		parityDisk := stripeNum % r.array.numDisks

		if diskIndex == parityDisk {
			if err := r.rebuildParityBlock(stripeNum, diskIndex); err != nil {
				r.array.disks[diskIndex].SetFailed(true)
				return fmt.Errorf("rebuild failed at stripe %d: %w", stripeNum, err)
			}
			rebuiltBlocks++
		} else {

			for offset := 0; offset < r.array.numDisks-1; offset++ {
				diskIdx := offset
				if diskIdx >= parityDisk {
					diskIdx++
				}

				if diskIdx == diskIndex {
					reconstructed, err := r.reconstructBlock(stripeNum, diskIndex, parityDisk)
					if err != nil {
						r.array.disks[diskIndex].SetFailed(true)
						return fmt.Errorf("rebuild failed reconstructing stripe %d: %w", stripeNum, err)
					}

					if err := r.array.disks[diskIndex].WriteBlock(stripeNum, reconstructed); err != nil {
						r.array.disks[diskIndex].SetFailed(true)
						return fmt.Errorf("rebuild failed writing stripe %d: %w", stripeNum, err)
					}
					rebuiltBlocks++
					break
				}
			}
		}

		if stripeNum%100 == 0 && stripeNum > 0 {
			fmt.Printf("[REBUILD] Progress: %d/%d stripes\n", stripeNum, maxStripes)
		}
	}

	fmt.Printf("[REBUILD] Disk %d rebuilt successfully (%d blocks)\n", diskIndex, rebuiltBlocks)
	return nil
}

func (r *raid5Impl) rebuildParityBlock(stripeNum, parityDisk int) error {
	parity := make([]byte, r.array.blockSize)

	for i := 0; i < r.array.numDisks; i++ {
		if i == parityDisk {
			continue
		}

		blockData, err := r.array.disks[i].ReadBlock(stripeNum)
		if err != nil {
			return fmt.Errorf("failed to read disk %d: %w", i, err)
		}

		xorBytes(parity, blockData)
	}

	return r.array.disks[parityDisk].WriteBlock(stripeNum, parity)
}

func xorBytes(dst, src []byte) {
	n := len(dst)
	if len(src) < n {
		n = len(src)
	}
	for i := 0; i < n; i++ {
		dst[i] ^= src[i]
	}
}
