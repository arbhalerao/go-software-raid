package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	level := flag.Int("level", 5, "RAID level (0, 1, or 5)")
	blockSize := flag.Int("block-size", 4096, "Block size in bytes")
	blocksPerDisk := flag.Int("blocks", 100, "Blocks per disk")
	flag.Parse()

	fmt.Println("─── RAID Demo ────────────────────────────")
	fmt.Println()

	raidLevel := RAIDLevel(*level)

	var numDisks int
	switch raidLevel {
	case RAID0:
		numDisks = 3
		fmt.Printf("RAID 0: Striping across %d disks — no redundancy, max performance\n", numDisks)
		fmt.Printf("Capacity: %d blocks\n\n", numDisks**blocksPerDisk)
	case RAID1:
		numDisks = 2
		fmt.Printf("RAID 1: Mirroring across %d disks — full redundancy\n", numDisks)
		fmt.Printf("Capacity: %d blocks\n\n", *blocksPerDisk)
	case RAID5:
		numDisks = 4
		fmt.Printf("RAID 5: Striping + distributed parity across %d disks — 1 disk fault tolerance\n", numDisks)
		fmt.Printf("Capacity: %d blocks\n\n", (numDisks-1)**blocksPerDisk)
	default:
		fmt.Printf("Unsupported RAID level: %d\n", raidLevel)
		os.Exit(1)
	}

	if err := os.MkdirAll(fmt.Sprintf("disks/raid%d", raidLevel), 0755); err != nil {
		fmt.Printf("Failed to create disk directory: %v\n", err)
		os.Exit(1)
	}

	diskPaths := make([]string, numDisks)
	for i := range diskPaths {
		diskPaths[i] = fmt.Sprintf("disks/raid%d/disk%d.img", raidLevel, i)
	}

	raid, err := NewRAIDArray(RAIDConfig{
		Level:         raidLevel,
		DiskPaths:     diskPaths,
		BlockSize:     *blockSize,
		BlocksPerDisk: *blocksPerDisk,
	})
	if err != nil {
		fmt.Printf("Failed to create RAID array: %v\n", err)
		os.Exit(1)
	}
	defer raid.Close()

	fmt.Println("RAID array created")
	fmt.Println()

	testBlocks := []struct {
		id   int
		data string
	}{
		{0, "hello from block zero"},
		{1, "disk two has the parity"},
		{2, "stripe width is four"},
		{3, "xor is just addition mod 2"},
		{4, "block four checking in"},
		{5, "last write wins nothing here"},
	}

	fmt.Println("─── Writing ──────────────────────────────")
	for _, tb := range testBlocks {
		data := make([]byte, *blockSize)
		copy(data, tb.data)
		if err := raid.WriteBlock(tb.id, data); err != nil {
			fmt.Printf("Block %d: %v\n", tb.id, err)
			os.Exit(1)
		}
		fmt.Printf("Block %d: %s\n", tb.id, tb.data)
	}
	fmt.Println()

	fmt.Println("─── Reading ──────────────────────────────")
	for _, tb := range testBlocks {
		data, err := raid.ReadBlock(tb.id)
		if err != nil {
			fmt.Printf("Block %d: %v\n", tb.id, err)
			os.Exit(1)
		}
		got := strings.TrimRight(string(data), "\x00")
		if got == tb.data {
			fmt.Printf("Block %d: %s\n", tb.id, got)
		} else {
			fmt.Printf("Block %d mismatch\n  want: %s\n  got:  %s\n", tb.id, tb.data, got)
		}
	}
	fmt.Println()

	fmt.Println("─── Disk Statistics ──────────────────────")
	for i, stat := range raid.GetStats() {
		status := "healthy"
		if stat.Failed {
			status = "FAILED"
		}
		fmt.Printf("Disk %d (%s): %s — reads: %d, writes: %d\n",
			i, stat.Path, status, stat.ReadCount, stat.WriteCount)
	}
	fmt.Println()
}
