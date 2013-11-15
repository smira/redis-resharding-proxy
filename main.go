package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

var (
	master_port int
	master_host string
	proxy_port  int
	proxy_host  string
)

const (
	bufSize int = 4096
)

func init() {
	flag.StringVar(&master_host, "master-host", "localhost", "Master Redis host")
	flag.IntVar(&master_port, "master-port", 6379, "Master Redis port")
	flag.StringVar(&proxy_host, "proxy-host", "", "Proxy host for listening, default is all hosts")
	flag.IntVar(&proxy_port, "proxy-port", 6380, "Proxy port for listening")
	flag.Parse()
}

func writeRedis(writer *bufio.Writer, reply string) (err error) {
	_, err = writer.WriteString(reply)
	if err != nil {
		return
	}
	err = writer.Flush()
	return
}

func masterConnection(repchan chan []byte) {
	defer close(repchan)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", master_host, master_port))
	if err != nil {
		log.Printf("Failed to connect to master: %v", err)
		return
	}

	reader := bufio.NewReaderSize(conn, bufSize)
	writer := bufio.NewWriterSize(conn, bufSize)

	err = writeRedis(writer, "SYNC\r\n")
	if err != nil {
		log.Printf("Failed to communicate to master: %v", err)
		return
	}

	bulk_header, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read bulk reply length: %v", err)
		return
	}

	if !strings.HasPrefix(bulk_header, "$") {
		log.Printf("Expected bulk transfer from master, but got: %#v", bulk_header)
		return
	}

	rdb_size, err := strconv.Atoi(strings.TrimSpace(bulk_header[1:]))

	log.Printf("RDB size: %d", rdb_size)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Print("Slave connection established from ", conn.RemoteAddr().String())

	reader := bufio.NewReaderSize(conn, bufSize)
	writer := bufio.NewWriterSize(conn, bufSize)

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error while reading from slave: %v", err)
			return
		}

		log.Printf("Got command: %#v", command)

		switch command {
		case "PING\r\n":
			err = writeRedis(writer, "+PONG\r\n")
			if err != nil {
				log.Printf("Failed to communicate with slave: %v", err)
				return
			}
		case "SYNC\r\n":
			// should start SYNC
			repchan := make(chan []byte, 100)
			go masterConnection(repchan)
			for {
				data := <-repchan
				_, err = conn.Write(data)
				if err != nil {
					log.Printf("Failed to write replication data to slave: %v", err)
					return
				}
			}
		default:
			err = writeRedis(writer, "+ERR unknown command\r\n")
			if err != nil {
				log.Printf("Failed to communicate with slave: %v", err)
				return
			}
		}
	}
}

func main() {
	log.Printf("Redis Resharding Proxy configured for Redis master at %s:%d", master_host, master_port)
	log.Printf("Waiting for connection from slave at %s:%d", proxy_host, proxy_port)

	// listen for incoming connection from Redis slave
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", proxy_host, proxy_port))
	if err != nil {
		log.Fatalf("Unable to listen: %v", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Unable to accept: %v", err)
			continue
		}

		go handleConnection(conn)
	}
}
