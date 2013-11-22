package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestReadRedisCommand(t *testing.T) {
	tests := []struct {
		description   string
		input         string
		expected      redisCommand
		expectedError error
	}{
		{
			description:   "1: Reply",
			input:         "+PONG\r\n",
			expected:      redisCommand{reply: "PONG"},
			expectedError: nil,
		},
		{
			description:   "2: Empty command",
			input:         "\n",
			expected:      redisCommand{},
			expectedError: nil,
		},
		{
			description:   "3: Simple command",
			input:         "SYNC\r\n",
			expected:      redisCommand{command: []string{"SYNC"}},
			expectedError: nil,
		},
		{
			description:   "4: Bulk reply",
			input:         "$4568\r\n",
			expected:      redisCommand{bulkSize: 4568},
			expectedError: nil,
		},
		{
			description:   "5: Complex command",
			input:         "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n",
			expected:      redisCommand{command: []string{"SET", "mykey", "myvalue"}},
			expectedError: nil,
		},
		{
			description:   "6: Immediate EOF",
			input:         "+PONG",
			expected:      redisCommand{},
			expectedError: fmt.Errorf("Failed to read command: %v", io.EOF),
		},
		{
			description:   "7: EOF in length",
			input:         "*3\r\n$3",
			expected:      redisCommand{},
			expectedError: fmt.Errorf("Failed to read command: %v", io.EOF),
		},
		{
			description:   "8: EOF in data",
			input:         "*3\r\n$3\r\nSE",
			expected:      redisCommand{},
			expectedError: fmt.Errorf("Failed to read argument: %v", io.ErrUnexpectedEOF),
		},
		{
			description:   "9: Unparsable length",
			input:         "*x\r\n",
			expected:      redisCommand{},
			expectedError: fmt.Errorf("Unable to parse command length: strconv.ParseInt: parsing \"x\": invalid syntax"),
		},
	}

	for _, test := range tests {
		test.expected.raw = []byte(test.input)

		command, err := readRedisCommand(bufio.NewReader(bytes.NewBufferString(test.input)))
		if err != nil {
			if test.expectedError == nil || test.expectedError.Error() != err.Error() {
				t.Errorf("Unexpected error: %v (test %s)", err, test.description)
			}
		} else if !reflect.DeepEqual(*command, test.expected) {
			t.Errorf("Output not equal to expected %#v != %#v (test %s)", *command, test.expected, test.description)
		}
	}
}
