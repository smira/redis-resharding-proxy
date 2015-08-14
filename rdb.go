package main

// Filter RDB file per spec: https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	rdbOpDB         = 0xFE
	rdbOpExpirySec  = 0xFD
	rdbOpExpiryMSec = 0xFC
	rdbOpEOF        = 0xFF

	rdbLen6Bit  = 0x0
	rdbLen14bit = 0x1
	rdbLen32Bit = 0x2
	rdbLenEnc   = 0x3

	rdbOpString    = 0x00
	rdbOpList      = 0x01
	rdbOpSet       = 0x02
	rdbOpZset      = 0x03
	rdbOpHash      = 0x04
	rdbOpZipmap    = 0x09
	rdbOpZiplist   = 0x0a
	rdbOpIntset    = 0x0b
	rdbOpSortedSet = 0x0c
	rdbOpHashmap   = 0x0d
)

var (
	rdbSignature = []byte{0x52, 0x45, 0x44, 0x49, 0x53}
)

var (
	// ErrWrongSignature is returned when RDB signature can't be parsed
	ErrWrongSignature = errors.New("rdb: wrong signature")
	// ErrVersionUnsupported is returned when RDB version is too high (can't parse)
	ErrVersionUnsupported = errors.New("rdb: version unsupported")
	// ErrUnsupportedOp is returned when unsupported operation is encountered in RDB
	ErrUnsupportedOp = errors.New("rdb: unsupported opcode")
	// ErrUnsupportedStringEnc is returned when unsupported string encoding is encountered in RDB
	ErrUnsupportedStringEnc = errors.New("rdb: unsupported string encoding")
)

// RDBFilter holds internal state of RDB filter while running
type RDBFilter struct {
	reader         *bufio.Reader
	output         chan<- []byte
	dissector      func(string) bool
	originalLength int64
	length         int64
	hash           uint64
	saved          []byte
	rdbVersion     int
	valueState     state
	shouldKeep     bool
	currentOp      byte
}

type state func(filter *RDBFilter) (nextstate state, err error)

// FilterRDB filters RDB file which is read from reader, sending chunks of data through output channel
// dissector function is applied to keys to check whether item should be kept or skipped
// length is original length of RDB file
func FilterRDB(reader *bufio.Reader, output chan<- []byte, dissector func(string) bool, length int64) (err error) {
	filter := &RDBFilter{
		reader:         reader,
		output:         output,
		dissector:      dissector,
		originalLength: length,
		shouldKeep:     true,
	}

	state := stateMagic

	for state != nil {
		state, err = state(filter)
		if err != nil {
			return
		}
	}

	return nil
}

// Read exactly n bytes
func (filter *RDBFilter) safeRead(n uint32) (result []byte, err error) {
	result = make([]byte, n)
	_, err = io.ReadFull(filter.reader, result)
	return
}

// Accumulate some data that might be either filtered out or passed through
func (filter *RDBFilter) write(data []byte) {
	if !filter.shouldKeep {
		return
	}

	if filter.saved == nil {
		filter.saved = make([]byte, len(data), 4096)
		copy(filter.saved, data)
	} else {
		filter.saved = append(filter.saved, data...)
	}
}

// Discard or keep saved data
func (filter *RDBFilter) keepOrDiscard() {
	if filter.shouldKeep && filter.saved != nil {
		filter.output <- filter.saved
		filter.hash = CRC64Update(filter.hash, filter.saved)
		filter.length += int64(len(filter.saved))
	}
	filter.saved = nil
	filter.shouldKeep = true
}

// Read length encoded prefix
func (filter *RDBFilter) readLength() (length uint32, encoding int8, err error) {
	prefix, err := filter.reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	filter.write([]byte{prefix})

	kind := (prefix & 0xC0) >> 6

	switch kind {
	case rdbLen6Bit:
		length = uint32(prefix & 0x3F)
		return length, -1, nil
	case rdbLen14bit:
		data, err := filter.reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		filter.write([]byte{data})
		length = ((uint32(prefix) & 0x3F) << 8) | uint32(data)
		return length, -1, nil
	case rdbLen32Bit:
		data, err := filter.safeRead(4)
		if err != nil {
			return 0, 0, err
		}
		filter.write(data)
		length = binary.BigEndian.Uint32(data)
		return length, -1, nil
	case rdbLenEnc:
		encoding = int8(prefix & 0x3F)
		return 0, encoding, nil
	}
	panic("never reached")
}

// Taken from Golly: https://github.com/tav/golly/blob/master/lzf/lzf.go
// Removed part that gets outputLength from data
func lzfDecompress(input []byte, outputLength uint32) (output []byte) {

	inputLength := uint32(len(input))

	var backref int64
	var ctrl, iidx, length, oidx uint32

	output = make([]byte, outputLength, outputLength)
	iidx = 0

	for iidx < inputLength {
		// Get the control byte.
		ctrl = uint32(input[iidx])
		iidx++

		if ctrl < (1 << 5) {
			// The control byte indicates a literal reference.
			ctrl++
			if oidx+ctrl > outputLength {
				return nil
			}

			// Safety check.
			if iidx+ctrl > inputLength {
				return nil
			}

			for {
				output[oidx] = input[iidx]
				iidx++
				oidx++
				ctrl--
				if ctrl == 0 {
					break
				}
			}
		} else {
			// The control byte indicates a back reference.
			length = ctrl >> 5
			backref = int64(oidx - ((ctrl & 31) << 8) - 1)

			// Safety check.
			if iidx >= inputLength {
				return nil
			}

			// It's an extended back reference. Read the extended length before
			// reading the full back reference location.
			if length == 7 {
				length += uint32(input[iidx])
				iidx++
				// Safety check.
				if iidx >= inputLength {
					return nil
				}
			}

			// Put together the full back reference location.
			backref -= int64(input[iidx])
			iidx++

			if oidx+length+2 > outputLength {
				return nil
			}

			if backref < 0 {
				return nil
			}

			output[oidx] = output[backref]
			oidx++
			backref++
			output[oidx] = output[backref]
			oidx++
			backref++

			for {
				output[oidx] = output[backref]
				oidx++
				backref++
				length--
				if length == 0 {
					break
				}
			}

		}
	}

	return output
}

// read string from RDB, only uncompressed version is supported
func (filter *RDBFilter) readString() (string, error) {
	var result string

	length, encoding, err := filter.readLength()
	if err != nil {
		return "", err
	}

	switch encoding {
	// length-prefixed string
	case -1:
		data, err := filter.safeRead(length)
		if err != nil {
			return "", err
		}
		filter.write(data)
		result = string(data)
	// integer as string
	case 0, 1, 2:
		data, err := filter.safeRead(1 << uint8(encoding))
		if err != nil {
			return "", err
		}
		filter.write(data)

		var num uint32

		if encoding == 0 {
			num = uint32(data[0])
		} else if encoding == 1 {
			num = uint32(data[0]) | (uint32(data[1]) << 8)
		} else if encoding == 2 {
			num = uint32(data[0]) | (uint32(data[1]) << 8) | (uint32(data[2]) << 16) | (uint32(data[3]) << 24)
		}

		result = fmt.Sprintf("%d", num)
	// compressed string
	case 3:
		clength, _, err := filter.readLength()
		if err != nil {
			return "", err
		}
		length, _, err := filter.readLength()
		if err != nil {
			return "", err
		}
		data, err := filter.safeRead(clength)
		if err != nil {
			return "", err
		}
		filter.write(data)

		result = string(lzfDecompress(data, length))
	default:
		return "", ErrUnsupportedStringEnc
	}

	return result, nil
}

// skip (copy) string from RDB
func (filter *RDBFilter) skipString() error {
	length, encoding, err := filter.readLength()
	if err != nil {
		return err
	}

	switch encoding {
	// length-prefixed string
	case -1:
		data, err := filter.safeRead(length)
		if err != nil {
			return err
		}
		filter.write(data)
	// integer as string
	case 0, 1, 2:
		data, err := filter.safeRead(1 << uint8(encoding))
		if err != nil {
			return err
		}
		filter.write(data)
	// compressed string
	case 3:
		clength, _, err := filter.readLength()
		if err != nil {
			return err
		}
		_, _, err = filter.readLength()
		if err != nil {
			return err
		}
		data, err := filter.safeRead(clength)
		if err != nil {
			return err
		}
		filter.write(data)
	default:
		return ErrUnsupportedStringEnc
	}
	return nil
}

// read RDB magic header
func stateMagic(filter *RDBFilter) (state, error) {
	signature, err := filter.safeRead(5)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(signature, rdbSignature) != 0 {
		return nil, ErrWrongSignature
	}
	filter.write(signature)

	versionRaw, err := filter.safeRead(4)
	if err != nil {
		return nil, err
	}
	version, err := strconv.Atoi(string(versionRaw))
	if err != nil {
		return nil, ErrWrongSignature
	}

	if version > 6 {
		return nil, ErrVersionUnsupported
	}

	filter.rdbVersion = version
	filter.write(versionRaw)
	filter.keepOrDiscard()

	return stateOp, nil
}

// main selector of operations
func stateOp(filter *RDBFilter) (state, error) {
	op, err := filter.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	filter.currentOp = op

	switch op {
	case rdbOpDB:
		filter.keepOrDiscard()
		return stateDB, nil
	case rdbOpExpirySec:
		return stateExpirySec, nil
	case rdbOpExpiryMSec:
		return stateExpiryMSec, nil
	case rdbOpString, rdbOpZipmap, rdbOpZiplist, rdbOpIntset, rdbOpSortedSet, rdbOpHashmap:
		filter.valueState = stateSkipString
		return stateKey, nil
	case rdbOpList, rdbOpSet:
		filter.valueState = stateSkipSetOrList
		return stateKey, nil
	case rdbOpZset:
		filter.valueState = stateSkipZset
		return stateKey, nil
	case rdbOpHash:
		filter.valueState = stateSkipHash
		return stateKey, nil
	case rdbOpEOF:
		filter.keepOrDiscard()
		filter.write([]byte{rdbOpEOF})
		filter.keepOrDiscard()
		if filter.rdbVersion > 4 {
			return stateCRC64, nil
		}
		return statePadding, nil
	default:
		return nil, ErrUnsupportedOp
	}
}

// DB index operation
func stateDB(filter *RDBFilter) (state, error) {
	filter.write([]byte{rdbOpDB})
	_, _, err := filter.readLength()
	if err != nil {
		return nil, err
	}
	filter.keepOrDiscard()

	return stateOp, nil
}

func stateExpirySec(filter *RDBFilter) (state, error) {
	expiry, err := filter.safeRead(4)
	if err != nil {
		return nil, err
	}

	filter.write([]byte{rdbOpExpirySec})
	filter.write(expiry)

	return stateOp, nil
}

func stateExpiryMSec(filter *RDBFilter) (state, error) {
	expiry, err := filter.safeRead(8)
	if err != nil {
		return nil, err
	}

	filter.write([]byte{rdbOpExpiryMSec})
	filter.write(expiry)

	return stateOp, nil
}

// read key
func stateKey(filter *RDBFilter) (state, error) {
	filter.write([]byte{filter.currentOp})
	key, err := filter.readString()
	if err != nil {
		return nil, err
	}

	filter.shouldKeep = filter.dissector(key)

	return filter.valueState, nil
}

// skip over string
func stateSkipString(filter *RDBFilter) (state, error) {
	err := filter.skipString()
	if err != nil {
		return nil, err
	}

	filter.keepOrDiscard()
	return stateOp, nil
}

// skip over set or list
func stateSkipSetOrList(filter *RDBFilter) (state, error) {
	length, _, err := filter.readLength()
	if err != nil {
		return nil, err
	}

	var i uint32

	for i = 0; i < length; i++ {
		// list element
		err = filter.skipString()
		if err != nil {
			return nil, err
		}
	}

	filter.keepOrDiscard()
	return stateOp, nil
}

// skip over hash
func stateSkipHash(filter *RDBFilter) (state, error) {
	length, _, err := filter.readLength()
	if err != nil {
		return nil, err
	}

	var i uint32

	for i = 0; i < length; i++ {
		// key
		err = filter.skipString()
		if err != nil {
			return nil, err
		}

		// value
		err = filter.skipString()
		if err != nil {
			return nil, err
		}
	}

	filter.keepOrDiscard()
	return stateOp, nil
}

// skip over zset
func stateSkipZset(filter *RDBFilter) (state, error) {
	length, _, err := filter.readLength()
	if err != nil {
		return nil, err
	}

	var i uint32

	for i = 0; i < length; i++ {
		err = filter.skipString()
		if err != nil {
			return nil, err
		}

		dlen, err := filter.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		filter.write([]byte{dlen})

		if dlen < 0xFD {
			double, err := filter.safeRead(uint32(dlen))
			if err != nil {
				return nil, err
			}

			filter.write(double)
		}
	}

	filter.keepOrDiscard()
	return stateOp, nil
}

// re-calculate crc64
func stateCRC64(filter *RDBFilter) (state, error) {
	_, err := filter.safeRead(8)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, filter.hash)
	filter.output <- buf
	filter.length += 8

	return statePadding, nil
}

// pad RDB with 0xFF up to original length
func statePadding(filter *RDBFilter) (state, error) {
	const paddingSize = 4096

	paddingLength := filter.originalLength - filter.length
	paddingBlock := make([]byte, paddingSize)

	for i := range paddingBlock {
		paddingBlock[i] = 0xFF
	}

	for paddingLength > 0 {
		if paddingLength > paddingSize {
			filter.output <- paddingBlock
			paddingLength -= paddingSize
		} else {
			filter.output <- paddingBlock[:paddingLength]
			break
		}
	}
	return nil, nil
}
