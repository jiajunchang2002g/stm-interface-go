package utils

// This file contains the I/O layer for the STM assignment.
// There should be no need to modify this file.

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

const MEMORY_CAPACITY = uint32(256);
const MAX_READ_WRITE_COUNT = uint32(32);

type StmCommandType uint8

const (
	START  StmCommandType = 'S'
	COMMIT StmCommandType = 'C'
	READ   StmCommandType = 'R'
	WRITE  StmCommandType = 'W'
)

type StmCommand struct {
	TransactionId uint32
	Address       uint32
	Value         uint32
	CommandType   StmCommandType
}

func ReadInput(conn net.Conn) (StmCommand, error) {
	var buf [16]byte
	_, err := io.ReadFull(conn, buf[:])
	if err != nil {
		return StmCommand{}, err
	}

	var cmd StmCommand
	cmd.TransactionId = binary.LittleEndian.Uint32(buf[0:4])
	cmd.Address = binary.LittleEndian.Uint32(buf[4:8])
	cmd.Value = binary.LittleEndian.Uint32(buf[8:12])
	cmd.CommandType = StmCommandType(buf[12])
	// bytes 13-15 are trailing padding; discard

	return cmd, nil
}

var printMu sync.Mutex

func PrintOutput(transactionId uint32, commands []StmCommand, outputTime int64) {
	printMu.Lock()
	defer printMu.Unlock()

	fmt.Printf("! id: %v timestamp: %v count: %v\n", transactionId, outputTime, len(commands))
	for _, cmd := range commands {
		fmt.Printf("%c @ %v -> %v\n", cmd.CommandType, cmd.Address, cmd.Value)
	}
}
