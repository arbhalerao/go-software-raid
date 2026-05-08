package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestRAID0Striping(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID0,
		DiskPaths:     []string{"disks/test_raid0_disk0.img", "disks/test_raid0_disk1.img", "disks/test_raid0_disk2.img"},
		BlockSize:     4096,
		BlocksPerDisk: 10,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	td := []struct {
		lb  int
		ed  int
		dat string
	}{
		{0, 0, "Block 0 on Disk 0"},
		{1, 1, "Block 1 on Disk 1"},
		{2, 2, "Block 2 on Disk 2"},
		{3, 0, "Block 3 on Disk 0"},
		{4, 1, "Block 4 on Disk 1"},
	}

	for _, x := range td {
		d := makeBlock(cfg.BlockSize, x.dat)
		if err := r.WriteBlock(x.lb, d); err != nil {
			t.Errorf("Failed to write block %d: %v", x.lb, err)
		}

		rd, err := r.ReadBlock(x.lb)
		if err != nil {
			t.Errorf("Failed to read block %d: %v", x.lb, err)
		}

		if !bytes.Equal(d, rd) {
			t.Errorf("Data mismatch for block %d", x.lb)
		}
	}
}

func TestRAID1Mirroring(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID1,
		DiskPaths:     []string{"disks/test_raid1_disk0.img", "disks/test_raid1_disk1.img"},
		BlockSize:     4096,
		BlocksPerDisk: 10,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	tb := makeBlock(cfg.BlockSize, "Mirrored data")
	if err := r.WriteBlock(0, tb); err != nil {
		t.Fatalf("Failed to write block: %v", err)
	}

	for i := 0; i < 2; i++ {
		d, err := r.disks[i].ReadBlock(0)
		if err != nil {
			t.Errorf("Failed to read from disk %d: %v", i, err)
		}
		if !bytes.Equal(tb, d) {
			t.Errorf("Disk %d does not have mirrored data", i)
		}
	}

	r.disks[0].SetFailed(true)
	rd, err := r.ReadBlock(0)
	if err != nil {
		t.Errorf("Failed to read in degraded mode: %v", err)
	}
	if !bytes.Equal(tb, rd) {
		t.Error("Data mismatch after disk failure")
	}
}

func TestRAID5ParityCalculation(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID5,
		DiskPaths:     []string{"disks/test_raid5_disk0.img", "disks/test_raid5_disk1.img", "disks/test_raid5_disk2.img", "disks/test_raid5_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	blks := []string{
		"RAID 5 block 0",
		"RAID 5 block 1",
		"RAID 5 block 2",
	}

	for i, c := range blks {
		d := makeBlock(cfg.BlockSize, c)
		if err := r.WriteBlock(i, d); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	for i, c := range blks {
		d, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Failed to read block %d: %v", i, err)
		}
		exp := makeBlock(cfg.BlockSize, c)
		if !bytes.Equal(exp, d) {
			t.Errorf("Data mismatch for block %d", i)
		}
	}
}

func TestRAID5DegradedMode(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID5,
		DiskPaths:     []string{"disks/test_raid5_deg_disk0.img", "disks/test_raid5_deg_disk1.img", "disks/test_raid5_deg_disk2.img", "disks/test_raid5_deg_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	blks := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		blks[i] = makeBlock(cfg.BlockSize, fmt.Sprintf("Test block %d", i))
		if err := r.WriteBlock(i, blks[i]); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	r.disks[1].SetFailed(true)

	for i := 0; i < 5; i++ {
		d, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Failed to read block %d in degraded mode: %v", i, err)
		}
		if !bytes.Equal(blks[i], d) {
			t.Errorf("Data mismatch for block %d in degraded mode", i)
		}
	}
}

func TestRAID5Rebuild(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID5,
		DiskPaths:     []string{"disks/test_raid5_reb_disk0.img", "disks/test_raid5_reb_disk1.img", "disks/test_raid5_reb_disk2.img", "disks/test_raid5_reb_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	blks := make([][]byte, 8)
	for i := 0; i < len(blks); i++ {
		blks[i] = makeBlock(cfg.BlockSize, fmt.Sprintf("Rebuild test block %d", i))
		if err := r.WriteBlock(i, blks[i]); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	fd := 2
	r.disks[fd].SetFailed(true)

	if err := r.RebuildDisk(fd); err != nil {
		t.Fatalf("Failed to rebuild disk: %v", err)
	}

	for i := 0; i < len(blks); i++ {
		d, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Failed to read block %d after rebuild: %v", i, err)
		}
		if !bytes.Equal(blks[i], d) {
			t.Errorf("Data mismatch for block %d after rebuild", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID1,
		DiskPaths:     []string{"disks/test_concurrent_disk0.img", "disks/test_concurrent_disk1.img"},
		BlockSize:     4096,
		BlocksPerDisk: 100,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	ng := 10
	bpg := 10

	for g := 0; g < ng; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < bpg; i++ {
				bid := gid*bpg + i
				d := makeBlock(cfg.BlockSize, fmt.Sprintf("Goroutine %d Block %d", gid, i))
				if err := r.WriteBlock(bid, d); err != nil {
					t.Errorf("Goroutine %d failed to write block %d: %v", gid, bid, err)
				}
			}
		}(g)
	}

	wg.Wait()

	for g := 0; g < ng; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < bpg; i++ {
				bid := gid*bpg + i
				if _, err := r.ReadBlock(bid); err != nil {
					t.Errorf("Goroutine %d failed to read block %d: %v", gid, bid, err)
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestBoundsChecking(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID0,
		DiskPaths:     []string{"disks/test_bounds_disk0.img", "disks/test_bounds_disk1.img"},
		BlockSize:     4096,
		BlocksPerDisk: 10,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID array: %v", err)
	}
	defer r.Close()

	d := makeBlock(cfg.BlockSize, "Test")

	if err := r.WriteBlock(-1, d); err == nil {
		t.Error("Expected error for negative block ID, got nil")
	}

	if err := r.WriteBlock(100, d); err == nil {
		t.Error("Expected error for out-of-bounds block ID, got nil")
	}

	if _, err := r.ReadBlock(-1); err == nil {
		t.Error("Expected error for negative block ID on read, got nil")
	}

	if _, err := r.ReadBlock(100); err == nil {
		t.Error("Expected error for out-of-bounds block ID on read, got nil")
	}
}

func TestXORProperties(t *testing.T) {
	a := []byte{0x12, 0x34, 0x56, 0x78}
	b := []byte{0xAB, 0xCD, 0xEF, 0x01}

	res := make([]byte, len(a))
	copy(res, a)

	xorBytes(res, b)
	xorBytes(res, b)

	if !bytes.Equal(res, a) {
		t.Error("XOR is not self-inverse")
	}

	dbs := [][]byte{
		{0x11, 0x22, 0x33, 0x44},
		{0x55, 0x66, 0x77, 0x88},
		{0x99, 0xAA, 0xBB, 0xCC},
	}

	p := make([]byte, 4)
	for _, blk := range dbs {
		xorBytes(p, blk)
	}

	rec := make([]byte, 4)
	copy(rec, p)
	xorBytes(rec, dbs[1])
	xorBytes(rec, dbs[2])

	if !bytes.Equal(rec, dbs[0]) {
		t.Error("XOR reconstruction failed")
	}
}

func TestRAID6ParityCalculation(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID6,
		DiskPaths:     []string{"disks/test_raid6_disk0.img", "disks/test_raid6_disk1.img", "disks/test_raid6_disk2.img", "disks/test_raid6_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID 6 array: %v", err)
	}
	defer r.Close()

	blks := []string{
		"RAID 6 block 0",
		"RAID 6 block 1",
		"RAID 6 block 2",
		"RAID 6 block 3",
	}

	for i, c := range blks {
		d := makeBlock(cfg.BlockSize, c)
		if err := r.WriteBlock(i, d); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	for i, c := range blks {
		d, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Failed to read block %d: %v", i, err)
		}
		if !bytes.Equal(makeBlock(cfg.BlockSize, c), d) {
			t.Errorf("Data mismatch for block %d", i)
		}
	}
}

func TestRAID6DegradedModeSingleDisk(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID6,
		DiskPaths:     []string{"disks/test_raid6_deg1_disk0.img", "disks/test_raid6_deg1_disk1.img", "disks/test_raid6_deg1_disk2.img", "disks/test_raid6_deg1_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID 6 array: %v", err)
	}
	defer r.Close()

	blks := make([][]byte, 6)
	for i := range blks {
		blks[i] = makeBlock(cfg.BlockSize, fmt.Sprintf("Degraded1 block %d", i))
		if err := r.WriteBlock(i, blks[i]); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	r.disks[2].SetFailed(true)

	for i, expected := range blks {
		got, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Block %d: read failed in degraded mode: %v", i, err)
			continue
		}
		if !bytes.Equal(expected, got) {
			t.Errorf("Block %d: data mismatch in degraded mode", i)
		}
	}
}

func TestRAID6DegradedModeQReconstruction(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID6,
		DiskPaths:     []string{"disks/test_raid6_degq_disk0.img", "disks/test_raid6_degq_disk1.img", "disks/test_raid6_degq_disk2.img", "disks/test_raid6_degq_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID 6 array: %v", err)
	}
	defer r.Close()

	b0 := makeBlock(cfg.BlockSize, "Q-reconstruction block 0")
	b1 := makeBlock(cfg.BlockSize, "Q-reconstruction block 1")
	if err := r.WriteBlock(0, b0); err != nil {
		t.Fatalf("Failed to write block 0: %v", err)
	}
	if err := r.WriteBlock(1, b1); err != nil {
		t.Fatalf("Failed to write block 1: %v", err)
	}

	r.disks[0].SetFailed(true)
	r.disks[2].SetFailed(true)

	got0, err := r.ReadBlock(0)
	if err != nil {
		t.Fatalf("Failed to read block 0 with P+data disk failed: %v", err)
	}
	if !bytes.Equal(b0, got0) {
		t.Error("Block 0: data mismatch after Q-based reconstruction")
	}

	got1, err := r.ReadBlock(1)
	if err != nil {
		t.Fatalf("Failed to read block 1: %v", err)
	}
	if !bytes.Equal(b1, got1) {
		t.Error("Block 1: data mismatch")
	}
}

func TestRAID6TwoDataDiskFailure(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID6,
		DiskPaths:     []string{"disks/test_raid6_two_disk0.img", "disks/test_raid6_two_disk1.img", "disks/test_raid6_two_disk2.img", "disks/test_raid6_two_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID 6 array: %v", err)
	}
	defer r.Close()

	b0 := makeBlock(cfg.BlockSize, "Two-disk-failure block 0")
	b1 := makeBlock(cfg.BlockSize, "Two-disk-failure block 1")
	if err := r.WriteBlock(0, b0); err != nil {
		t.Fatalf("Failed to write block 0: %v", err)
	}
	if err := r.WriteBlock(1, b1); err != nil {
		t.Fatalf("Failed to write block 1: %v", err)
	}

	r.disks[2].SetFailed(true)
	r.disks[3].SetFailed(true)

	got0, err := r.ReadBlock(0)
	if err != nil {
		t.Fatalf("Failed to read block 0 with two data disks failed: %v", err)
	}
	if !bytes.Equal(b0, got0) {
		t.Error("Block 0: data mismatch after two-disk reconstruction")
	}

	got1, err := r.ReadBlock(1)
	if err != nil {
		t.Fatalf("Failed to read block 1 with two data disks failed: %v", err)
	}
	if !bytes.Equal(b1, got1) {
		t.Error("Block 1: data mismatch after two-disk reconstruction")
	}
}

func TestRAID6Rebuild(t *testing.T) {
	cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := RAIDConfig{
		Level:         RAID6,
		DiskPaths:     []string{"disks/test_raid6_reb_disk0.img", "disks/test_raid6_reb_disk1.img", "disks/test_raid6_reb_disk2.img", "disks/test_raid6_reb_disk3.img"},
		BlockSize:     4096,
		BlocksPerDisk: 20,
	}

	r, err := NewRAIDArray(cfg)
	if err != nil {
		t.Fatalf("Failed to create RAID 6 array: %v", err)
	}
	defer r.Close()

	blks := make([][]byte, 8)
	for i := range blks {
		blks[i] = makeBlock(cfg.BlockSize, fmt.Sprintf("RAID6 rebuild block %d", i))
		if err := r.WriteBlock(i, blks[i]); err != nil {
			t.Fatalf("Failed to write block %d: %v", i, err)
		}
	}

	failedDisk := 2
	r.disks[failedDisk].SetFailed(true)

	if err := r.RebuildDisk(failedDisk); err != nil {
		t.Fatalf("Rebuild failed: %v", err)
	}

	for i, expected := range blks {
		got, err := r.ReadBlock(i)
		if err != nil {
			t.Errorf("Block %d: read failed after rebuild: %v", i, err)
			continue
		}
		if !bytes.Equal(expected, got) {
			t.Errorf("Block %d: data mismatch after rebuild", i)
		}
	}
}

func TestGF8Arithmetic(t *testing.T) {
	for x := 0; x < 256; x++ {
		if got := gfMul(1, byte(x)); got != byte(x) {
			t.Errorf("gfMul(1, %d) = %d, want %d", x, got, x)
		}
	}

	for a := 0; a < 256; a++ {
		if got := gfMul(byte(a), 0); got != 0 {
			t.Errorf("gfMul(%d, 0) = %d, want 0", a, got)
		}
	}

	for a := 1; a < 256; a++ {
		if got := gfMul(byte(a), gfInv(byte(a))); got != 1 {
			t.Errorf("gfMul(%d, gfInv(%d)) = %d, want 1", a, a, got)
		}
	}

	if got := gfPow(2, 0); got != 1 {
		t.Errorf("gfPow(2, 0) = %d, want 1", got)
	}

	if got := gfPow(2, 255); got != 1 {
		t.Errorf("gfPow(2, 255) = %d, want 1", got)
	}

	a, b, c := byte(0x53), byte(0xCA), byte(0x7F)
	lhs := gfMul(a, b^c)
	rhs := gfMul(a, b) ^ gfMul(a, c)
	if lhs != rhs {
		t.Errorf("GF distributivity failed: gfMul(%#x, %#x^%#x)=%#x, want %#x", a, b, c, lhs, rhs)
	}
}

func setupTestEnv(t *testing.T) func() {
	if err := os.MkdirAll("disks", 0755); err != nil {
		t.Fatalf("Failed to create disk directory: %v", err)
	}
	return func() {
		files, _ := os.ReadDir("disk")
		for _, f := range files {
			if len(f.Name()) > 5 && f.Name()[:5] == "test_" {
				os.Remove("disks/" + f.Name())
			}
		}
	}
}

func makeBlock(sz int, s string) []byte {
	b := make([]byte, sz)
	copy(b, []byte(s))
	return b
}
