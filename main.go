package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	masterPort int
	masterHost string
	proxyPort  int
	proxyHost  string
	keyRegexp  *regexp.Regexp
)

const (
	bufSize       = 16384
	channelBuffer = 100
)

type redisCommand struct {
	raw      []byte
	command  []string
	reply    string
	bulkSize int64
}

func readRedisCommand(reader *bufio.Reader) (*redisCommand, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Failed to read command: %v", err)
	}

	if header == "\n" || header == "\r\n" {
		// empty command
		return &redisCommand{raw: []byte(header)}, nil
	}

	if strings.HasPrefix(header, "+") {
		return &redisCommand{raw: []byte(header), reply: strings.TrimSpace(header[1:])}, nil
	}

	if strings.HasPrefix(header, "$") {
		bulkSize, err := strconv.ParseInt(strings.TrimSpace(header[1:]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Unable to decode bulk size: %v", err)
		}
		return &redisCommand{raw: []byte(header), bulkSize: bulkSize}, nil
	}

	if strings.HasPrefix(header, "*") {
		cmdSize, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return nil, fmt.Errorf("Unable to parse command length: %v", err)
		}

		result := &redisCommand{raw: []byte(header), command: make([]string, cmdSize)}

		for i := range result.command {
			header, err = reader.ReadString('\n')
			if !strings.HasPrefix(header, "$") || err != nil {
				return nil, fmt.Errorf("Failed to read command: %v", err)
			}

			result.raw = append(result.raw, []byte(header)...)

			argSize, err := strconv.Atoi(strings.TrimSpace(header[1:]))
			if err != nil {
				return nil, fmt.Errorf("Unable to parse argument length: %v", err)
			}

			argument := make([]byte, argSize)
			_, err = io.ReadFull(reader, argument)
			if err != nil {
				return nil, fmt.Errorf("Failed to read argument: %v", err)
			}

			result.raw = append(result.raw, argument...)

			header, err = reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("Failed to read argument: %v", err)
			}

			result.raw = append(result.raw, []byte(header)...)

			result.command[i] = string(argument)
		}

		return result, nil
	}

	return &redisCommand{raw: []byte(header), command: []string{strings.TrimSpace(header)}}, nil
}

// Goroutine that handles writing commands to master
func masterWriter(conn net.Conn, masterchannel <-chan []byte) {
	defer conn.Close()

	for data := range masterchannel {
		_, err := conn.Write(data)
		if err != nil {
			log.Printf("Failed to write data to master: %v\n", err)
			return
		}
	}
}

// Connect to master, request replication and filter it
func masterConnection(slavechannel chan<- []byte, masterchannel <-chan []byte) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", masterHost, masterPort))
	if err != nil {
		log.Printf("Failed to connect to master: %v\n", err)
		return
	}

	defer conn.Close()
	go masterWriter(conn, masterchannel)

	reader := bufio.NewReaderSize(conn, bufSize)

	for {
		command, err := readRedisCommand(reader)
		if err != nil {
			log.Printf("Error while reading from master: %v\n", err)
			return
		}

		if command.reply != "" || command.command == nil && command.bulkSize == 0 {
			// passthrough reply & empty command
			slavechannel <- command.raw
			slavechannel <- nil
		} else if len(command.command) == 1 && command.command[0] == "PING" {
			log.Println("Got PING from master")

			slavechannel <- command.raw
			slavechannel <- nil
		} else if command.bulkSize > 0 {
			// RDB Transfer

			log.Printf("RDB size: %d\n", command.bulkSize)

			slavechannel <- command.raw

			err = FilterRDB(reader, slavechannel, func(key string) bool { return keyRegexp.FindStringIndex(key) != nil }, command.bulkSize)
			if err != nil {
				log.Printf("Unable to read RDB: %v\n", err)
				return
			}

			log.Println("RDB filtering finished, filtering commands...")
		} else {
			if len(command.command) >= 2 && keyRegexp.FindStringIndex(command.command[1]) == nil {
				continue
			}

			slavechannel <- command.raw
			slavechannel <- nil
		}

	}
}

// Goroutine that handles writing data back to slave
func slaveWriter(conn net.Conn, slavechannel <-chan []byte) {
	writer := bufio.NewWriterSize(conn, bufSize)

	for data := range slavechannel {
		var err error

		if data == nil {
			err = writer.Flush()
		} else {
			_, err = writer.Write(data)
		}

		if err != nil {
			log.Printf("Failed to write data to slave: %v\n", err)
			return
		}
	}
}

// Read commands from slave
func slaveReader(conn net.Conn) {
	defer conn.Close()

	log.Print("Slave connection established from ", conn.RemoteAddr().String())

	reader := bufio.NewReaderSize(conn, bufSize)

	// channel for writing to slave
	slavechannel := make(chan []byte, channelBuffer)
	defer close(slavechannel)

	// channel for writing to master
	masterchannel := make(chan []byte, channelBuffer)
	defer close(masterchannel)

	go slaveWriter(conn, slavechannel)
	go masterConnection(slavechannel, masterchannel)

	for {
		command, err := readRedisCommand(reader)
		if err != nil {
			log.Printf("Error while reading from slave: %v\n", err)
			return
		}

		if command.reply != "" || command.command == nil && command.bulkSize == 0 {
			// passthrough reply & empty command
			masterchannel <- command.raw
		} else if len(command.command) == 1 && command.command[0] == "PING" {
			log.Println("Got PING from slave")

			masterchannel <- command.raw
		} else if len(command.command) == 1 && command.command[0] == "SYNC" {
			log.Println("Starting SYNC")

			masterchannel <- command.raw
		} else if len(command.command) == 3 && command.command[0] == "REPLCONF" && command.command[1] == "ACK" {
			log.Println("Got ACK from slave")

			masterchannel <- command.raw
		} else {
			// unknown command
			slavechannel <- []byte("+ERR unknown command\r\n")
			slavechannel <- nil
		}
	}
}

func main() {
	flag.StringVar(&masterHost, "master-host", "localhost", "Master Redis host")
	flag.IntVar(&masterPort, "master-port", 6379, "Master Redis port")
	flag.StringVar(&proxyHost, "proxy-host", "", "Proxy listening interface, default is on all interfaces")
	flag.IntVar(&proxyPort, "proxy-port", 6380, "Proxy port for listening")
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "Please specify regular expression to match against the Redis keys as the only argument.")
		os.Exit(1)
	}

	var err error
	keyRegexp, err = regexp.Compile(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Wrong format of regular expression: %v", err)
		os.Exit(1)
	}

	log.Printf("Redis Resharding Proxy configured for Redis master at %s:%d\n", masterHost, masterPort)
	log.Printf("Waiting for connection from slave at %s:%d\n", proxyHost, proxyPort)

	// listen for incoming connection from Redis slave
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", proxyHost, proxyPort))
	if err != nil {
		log.Fatalf("Unable to listen: %v\n", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Unable to accept: %v\n", err)
			continue
		}

		go slaveReader(conn)
	}
}
