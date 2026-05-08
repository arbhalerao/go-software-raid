package main

import (
	"fmt"
	"sync"
)

func gfMul(a, b byte) byte {
	var result byte
	for b != 0 {
		if b&1 != 0 {
			result ^= a
		}
		if a&0x80 != 0 {
			a = (a << 1) ^ 0x1D
		} else {
			a <<= 1
		}
		b >>= 1
	}
	return result
}

func gfPow(a byte, n int) byte {
	result := byte(1)
	for n > 0 {
		if n&1 != 0 {
			result = gfMul(result, a)
		}
		a = gfMul(a, a)
		n >>= 1
	}
	return result
}

func gfInv(a byte) byte {
	if a == 0 {
		panic("gfInv: inverse of 0 is undefined")
	}
	return gfPow(a, 254)
}

type raid6Impl struct {
	array *RAIDArray
	mu    sync.Mutex
}

func newRAID6(array *RAIDArray) *raid6Impl {
	return &raid6Impl{array: array}
}

func (r *raid6Impl) stripeLayout(stripeNum int) (pDisk, qDisk int, dataDisks []int) {
	n := r.array.numDisks
	pDisk = stripeNum % n
	qDisk = (stripeNum + 1) % n
	dataDisks = make([]int, 0, n-2)
	for i := 0; i < n; i++ {
		if i != pDisk && i != qDisk {
			dataDisks = append(dataDisks, i)
		}
	}
	return
}

func (r *raid6Impl) writeBlock(logicalBlockID int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	dataPerStripe := r.array.numDisks - 2
	stripeNum := logicalBlockID / dataPerStripe
	stripeOffset := logicalBlockID % dataPerStripe

	pDisk, qDisk, dataDisks := r.stripeLayout(stripeNum)
	targetDisk := dataDisks[stripeOffset]
	blockSize := r.array.blockSize

	stripeData := make([][]byte, dataPerStripe)
	for i, diskIdx := range dataDisks {
		if i == stripeOffset {
			stripeData[i] = data
			continue
		}
		if r.array.disks[diskIdx].IsFailed() {
			stripeData[i] = make([]byte, blockSize)
			continue
		}
		block, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
		if err != nil {
			stripeData[i] = make([]byte, blockSize)
		} else {
			stripeData[i] = block
		}
	}

	p := make([]byte, blockSize)
	for _, d := range stripeData {
		xorBytes(p, d)
	}

	q := make([]byte, blockSize)
	for i, d := range stripeData {
		coeff := gfPow(2, i)
		for j := range q {
			q[j] ^= gfMul(coeff, d[j])
		}
	}

	if !r.array.disks[targetDisk].IsFailed() {
		if err := r.array.disks[targetDisk].WriteBlock(stripeNum, data); err != nil {
			return fmt.Errorf("failed to write data to disk %d: %w", targetDisk, err)
		}
	}
	if !r.array.disks[pDisk].IsFailed() {
		if err := r.array.disks[pDisk].WriteBlock(stripeNum, p); err != nil {
			return fmt.Errorf("failed to write P parity to disk %d: %w", pDisk, err)
		}
	}
	if !r.array.disks[qDisk].IsFailed() {
		if err := r.array.disks[qDisk].WriteBlock(stripeNum, q); err != nil {
			return fmt.Errorf("failed to write Q parity to disk %d: %w", qDisk, err)
		}
	}
	return nil
}

func (r *raid6Impl) readBlock(logicalBlockID int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dataPerStripe := r.array.numDisks - 2
	stripeNum := logicalBlockID / dataPerStripe
	stripeOffset := logicalBlockID % dataPerStripe

	pDisk, qDisk, dataDisks := r.stripeLayout(stripeNum)
	targetDisk := dataDisks[stripeOffset]

	if !r.array.disks[targetDisk].IsFailed() {
		return r.array.disks[targetDisk].ReadBlock(stripeNum)
	}

	fmt.Printf("  [RAID6] Degraded read: reconstructing block %d from parity\n", logicalBlockID)
	return r.reconstructDataBlock(stripeNum, stripeOffset, pDisk, qDisk, dataDisks)
}

func (r *raid6Impl) reconstructDataBlock(stripeNum, stripeOffset, pDisk, qDisk int, dataDisks []int) ([]byte, error) {
	pFailed := r.array.disks[pDisk].IsFailed()
	qFailed := r.array.disks[qDisk].IsFailed()

	otherFailedOffset := -1
	failCount := 1
	if pFailed {
		failCount++
	}
	if qFailed {
		failCount++
	}
	for i, diskIdx := range dataDisks {
		if i == stripeOffset {
			continue
		}
		if r.array.disks[diskIdx].IsFailed() {
			failCount++
			otherFailedOffset = i
		}
	}

	if failCount > 2 {
		return nil, fmt.Errorf("RAID6: %d disks failed, cannot recover (max 2)", failCount)
	}

	switch {
	case otherFailedOffset >= 0:
		return r.reconstructTwoDataDisks(stripeNum, stripeOffset, otherFailedOffset, pDisk, qDisk, dataDisks)
	case !pFailed:
		return r.reconstructUsingP(stripeNum, stripeOffset, pDisk, dataDisks)
	default:
		return r.reconstructUsingQ(stripeNum, stripeOffset, qDisk, dataDisks)
	}
}

func (r *raid6Impl) reconstructUsingP(stripeNum, stripeOffset, pDisk int, dataDisks []int) ([]byte, error) {
	p, err := r.array.disks[pDisk].ReadBlock(stripeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read P parity from disk %d: %w", pDisk, err)
	}
	result := make([]byte, r.array.blockSize)
	copy(result, p)
	for i, diskIdx := range dataDisks {
		if i == stripeOffset || r.array.disks[diskIdx].IsFailed() {
			continue
		}
		block, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read disk %d: %w", diskIdx, err)
		}
		xorBytes(result, block)
	}
	return result, nil
}

func (r *raid6Impl) reconstructUsingQ(stripeNum, stripeOffset, qDisk int, dataDisks []int) ([]byte, error) {
	q, err := r.array.disks[qDisk].ReadBlock(stripeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read Q parity from disk %d: %w", qDisk, err)
	}
	remainder := make([]byte, r.array.blockSize)
	copy(remainder, q)
	for i, diskIdx := range dataDisks {
		if i == stripeOffset || r.array.disks[diskIdx].IsFailed() {
			continue
		}
		block, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read disk %d: %w", diskIdx, err)
		}
		coeff := gfPow(2, i)
		for j := range remainder {
			remainder[j] ^= gfMul(coeff, block[j])
		}
	}
	invCoeff := gfInv(gfPow(2, stripeOffset))
	result := make([]byte, r.array.blockSize)
	for j := range result {
		result[j] = gfMul(invCoeff, remainder[j])
	}
	return result, nil
}

func (r *raid6Impl) reconstructTwoDataDisks(stripeNum, diOffset, djOffset, pDisk, qDisk int, dataDisks []int) ([]byte, error) {
	blockSize := r.array.blockSize

	p, err := r.array.disks[pDisk].ReadBlock(stripeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read P parity: %w", err)
	}
	q, err := r.array.disks[qDisk].ReadBlock(stripeNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read Q parity: %w", err)
	}

	A := make([]byte, blockSize)
	copy(A, p)
	B := make([]byte, blockSize)
	copy(B, q)

	for i, diskIdx := range dataDisks {
		if i == diOffset || i == djOffset {
			continue
		}
		block, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
		if err != nil {
			return nil, fmt.Errorf("failed to read disk %d: %w", diskIdx, err)
		}
		xorBytes(A, block)
		coeff := gfPow(2, i)
		for j := range B {
			B[j] ^= gfMul(coeff, block[j])
		}
	}

	gdi := gfPow(2, diOffset)
	gdj := gfPow(2, djOffset)
	denom := gdi ^ gdj
	if denom == 0 {
		return nil, fmt.Errorf("RAID6: degenerate GF system (identical coefficients for offsets %d and %d)", diOffset, djOffset)
	}
	invDenom := gfInv(denom)
	Ddj := make([]byte, blockSize)
	for j := range Ddj {
		Ddj[j] = gfMul(invDenom, B[j]^gfMul(gdi, A[j]))
	}

	Ddi := make([]byte, blockSize)
	copy(Ddi, A)
	xorBytes(Ddi, Ddj)
	return Ddi, nil
}

func (r *raid6Impl) rebuildDisk(diskIndex int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if diskIndex < 0 || diskIndex >= r.array.numDisks {
		return fmt.Errorf("invalid disk index %d", diskIndex)
	}
	if !r.array.disks[diskIndex].IsFailed() {
		return fmt.Errorf("disk %d is not marked as failed", diskIndex)
	}

	fmt.Printf("\n[REBUILD] Starting RAID6 rebuild of disk %d...\n", diskIndex)

	disk := r.array.disks[diskIndex]
	disk.SetFailed(false)

	numStripes := disk.Capacity()
	blockSize := r.array.blockSize
	rebuiltBlocks := 0

	for stripeNum := 0; stripeNum < numStripes; stripeNum++ {
		pDisk, qDisk, dataDisks := r.stripeLayout(stripeNum)

		var block []byte

		switch {
		case diskIndex == pDisk:
			block = make([]byte, blockSize)
			for _, diskIdx := range dataDisks {
				d, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
				if err != nil {
					disk.SetFailed(true)
					return fmt.Errorf("rebuild disk %d: read disk %d stripe %d: %w", diskIndex, diskIdx, stripeNum, err)
				}
				xorBytes(block, d)
			}

		case diskIndex == qDisk:
			block = make([]byte, blockSize)
			for i, diskIdx := range dataDisks {
				d, err := r.array.disks[diskIdx].ReadBlock(stripeNum)
				if err != nil {
					disk.SetFailed(true)
					return fmt.Errorf("rebuild disk %d: read disk %d stripe %d: %w", diskIndex, diskIdx, stripeNum, err)
				}
				coeff := gfPow(2, i)
				for j := range block {
					block[j] ^= gfMul(coeff, d[j])
				}
			}

		default:
			disk.SetFailed(true)
			var offset int
			for i, d := range dataDisks {
				if d == diskIndex {
					offset = i
					break
				}
			}
			var err error
			block, err = r.reconstructDataBlock(stripeNum, offset, pDisk, qDisk, dataDisks)
			disk.SetFailed(false)
			if err != nil {
				disk.SetFailed(true)
				return fmt.Errorf("rebuild disk %d: reconstruct stripe %d: %w", diskIndex, stripeNum, err)
			}
		}

		if err := disk.WriteBlock(stripeNum, block); err != nil {
			disk.SetFailed(true)
			return fmt.Errorf("rebuild disk %d: write stripe %d: %w", diskIndex, stripeNum, err)
		}
		rebuiltBlocks++

		if stripeNum%100 == 0 && stripeNum > 0 {
			fmt.Printf("[REBUILD] Progress: %d/%d stripes\n", stripeNum, numStripes)
		}
	}

	fmt.Printf("[REBUILD] Disk %d rebuilt successfully (%d blocks)\n", diskIndex, rebuiltBlocks)
	return nil
}
