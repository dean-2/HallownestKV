package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	OpPut       byte = 0x01
	OpGet       byte = 0x02
	OpTombstone byte = 0x03
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "put":
		putCmd := flag.NewFlagSet("put", flag.ExitOnError)
		addr := putCmd.String("addr", "127.0.0.1:50051", "Cluster node address")
		key := putCmd.String("key", "", "Key to insert/update")
		val := putCmd.String("value", "", "Value to store")
		_ = putCmd.Parse(os.Args[2:])

		if *key == "" {
			fmt.Println("Error: --key is required")
			os.Exit(1)
		}

		executePut(*addr, []byte(*key), []byte(*val))

	case "get":
		getCmd := flag.NewFlagSet("get", flag.ExitOnError)
		addr := getCmd.String("addr", "127.0.0.1:50051", "Cluster node address")
		key := getCmd.String("key", "", "Key to query")
		_ = getCmd.Parse(os.Args[2:])

		if *key == "" {
			fmt.Println("Error: --key is required")
			os.Exit(1)
		}

		executeGet(*addr, []byte(*key))

	case "delete":
		delCmd := flag.NewFlagSet("delete", flag.ExitOnError)
		addr := delCmd.String("addr", "127.0.0.1:50051", "Cluster node address")
		key := delCmd.String("key", "", "Key to delete")
		_ = delCmd.Parse(os.Args[2:])

		if *key == "" {
			fmt.Println("Error: --key is required")
			os.Exit(1)
		}

		executeDelete(*addr, []byte(*key))

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("HallownestKV CLI Client (Stagway Transport)")
	fmt.Println("Usage:")
	fmt.Println("  hallownest-cli put --key <key> --value <value> [--addr 127.0.0.1:50051]")
	fmt.Println("  hallownest-cli get --key <key> [--addr 127.0.0.1:50051]")
	fmt.Println("  hallownest-cli delete --key <key> [--addr 127.0.0.1:50051]")
}

func executePut(addr string, key, val []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Connection error to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	keyLen := uint32(len(key))
	valLen := uint32(len(val))
	reqBuf := make([]byte, 9+keyLen+valLen)

	reqBuf[0] = OpPut
	binary.BigEndian.PutUint32(reqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(reqBuf[5:9], valLen)
	copy(reqBuf[9:9+keyLen], key)
	copy(reqBuf[9+keyLen:], val)

	if _, err := conn.Write(reqBuf); err != nil {
		fmt.Printf("Failed sending Put request: %v\n", err)
		os.Exit(1)
	}

	respBuf := make([]byte, 10)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		fmt.Printf("Failed reading response: %v\n", err)
		os.Exit(1)
	}

	success := respBuf[0] == 1
	index := binary.BigEndian.Uint32(respBuf[1:5])
	term := binary.BigEndian.Uint32(respBuf[5:9])
	isLeader := respBuf[9] == 1

	if success {
		fmt.Printf("OK (Index: %d, Term: %d, Leader: %v)\n", index, term, isLeader)
	} else {
		fmt.Printf("FAILED (Not Leader or Cluster Error, Leader: %v)\n", isLeader)
	}
}

func executeGet(addr string, key []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Connection error to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	keyLen := uint32(len(key))
	reqBuf := make([]byte, 9+keyLen)

	reqBuf[0] = OpGet
	binary.BigEndian.PutUint32(reqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(reqBuf[5:9], 0)
	copy(reqBuf[9:], key)

	if _, err := conn.Write(reqBuf); err != nil {
		fmt.Printf("Failed sending Get request: %v\n", err)
		os.Exit(1)
	}

	hdrBuf := make([]byte, 6)
	if _, err := io.ReadFull(conn, hdrBuf); err != nil {
		fmt.Printf("Failed reading header: %v\n", err)
		os.Exit(1)
	}

	found := hdrBuf[0] == 1
	tombstone := hdrBuf[1] == 1
	valLen := binary.BigEndian.Uint32(hdrBuf[2:6])

	if !found {
		fmt.Println("404 Not Found")
		return
	}

	if tombstone {
		fmt.Println("KEY DELETED (Tombstone)")
		return
	}

	valBuf := make([]byte, valLen)
	if _, err := io.ReadFull(conn, valBuf); err != nil {
		fmt.Printf("Failed reading value: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("VALUE: %s\n", string(valBuf))
}

func executeDelete(addr string, key []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Connection error to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	keyLen := uint32(len(key))
	reqBuf := make([]byte, 9+keyLen)

	reqBuf[0] = OpTombstone
	binary.BigEndian.PutUint32(reqBuf[1:5], keyLen)
	binary.BigEndian.PutUint32(reqBuf[5:9], 0)
	copy(reqBuf[9:], key)

	if _, err := conn.Write(reqBuf); err != nil {
		fmt.Printf("Failed sending Delete request: %v\n", err)
		os.Exit(1)
	}

	respBuf := make([]byte, 10)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		fmt.Printf("Failed reading response: %v\n", err)
		os.Exit(1)
	}

	success := respBuf[0] == 1
	index := binary.BigEndian.Uint32(respBuf[1:5])
	term := binary.BigEndian.Uint32(respBuf[5:9])

	if success {
		fmt.Printf("TOMBSTONE INSERTED (Index: %d, Term: %d)\n", index, term)
	} else {
		fmt.Println("FAILED")
	}
}
