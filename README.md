# go-software-raid

Software RAID 0, 1, and 5 implemented in Go. Disks are backed by flat files, blocks are read/written through the RAID abstraction layer.

## RAID levels

- **RAID 0** — striping across 3 disks, no redundancy
- **RAID 1** — mirroring across 2 disks, full redundancy
- **RAID 5** — striping + distributed parity across 4 disks, survives one disk failure

## Run

```
go run . -level 0
go run . -level 5
go run . -level 1
```

Flags:
- `-level` — RAID level (default: 5)
- `-block-size` — block size in bytes (default: 4096)
- `-blocks` — blocks per disk (default: 100)

Disk images are created under `disks/raid<level>/`.

## Test

```
go test ./...
```
