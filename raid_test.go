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
