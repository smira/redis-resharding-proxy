package main

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestFilterRDB(t *testing.T) {
	tests := []struct {
		description   string
		rdb           string
		expected      string
		expectedError error
		filter        func(string) bool
	}{
		{
			description:   "1: Simple RDB, no filter",
			rdb:           RDBFile1,
			expected:      RDBFile1,
			expectedError: nil,
			filter:        func(string) bool { return true },
		},
		{
			description:   "2: Simple RDB, filter out b_",
			rdb:           RDBFile1,
			expected:      "REDIS0006\xfe\x00\x00\x03a_1\x04lala\x00\x03a_2\xc0!\xff\xad}0`\xa6\xf4\xa1\xab",
			expectedError: nil,
			filter:        func(key string) bool { return strings.HasPrefix(key, "a_") },
		},
		{
			description:   "3: RDB broken, no magic",
			rdb:           "NOTREDIS",
			expected:      "",
			expectedError: ErrWrongSignature,
			filter:        func(string) bool { return true },
		},
		{
			description:   "4: RDB version unsupported",
			rdb:           "REDIS0007",
			expected:      "",
			expectedError: ErrVersionUnsupported,
			filter:        func(string) bool { return true },
		},
		{
			description:   "5: RDB too short",
			rdb:           "REDIS0006\xfe\x00\x00\x03",
			expected:      "",
			expectedError: io.EOF,
			filter:        func(string) bool { return true },
		},
		{
			description:   "6: RDB too short",
			rdb:           "REDIS0006\xfe",
			expected:      "",
			expectedError: io.EOF,
			filter:        func(string) bool { return true },
		},
		{
			description:   "7: RDB too short",
			rdb:           "REDIS0006",
			expected:      "",
			expectedError: io.EOF,
			filter:        func(string) bool { return true },
		},
		{
			description:   "8: RDB too short",
			rdb:           "REDIS00",
			expected:      "",
			expectedError: io.EOF,
			filter:        func(string) bool { return true },
		},
		{
			description: "9: Old RDB, many types, no filtering",
			rdb:         RDBFile2,
			expected:    RDBFile2,
			filter:      func(string) bool { return true },
		},
	}

	for _, test := range tests {
		ch := make(chan []byte)
		hadError := false

		go func() {
			err := FilterRDB(bufio.NewReader(bytes.NewBufferString(test.rdb)), ch, test.filter)
			if err != nil {
				if test.expectedError == nil || test.expectedError != err {
					t.Errorf("Filtering failed (%s): %v", test.description, err)
				} else {
					hadError = true
				}

			}
		}()

		received := ""

		for {
			data, ok := <-ch
			if !ok {
				break
			}
			received += string(data)
		}

		if test.expected != "" && test.expected != received {
			t.Errorf("output not equal to expected: %#v != %#v (test %s)", test.expected, received, test.description)
		}

		if test.expectedError != nil && !hadError {
			t.Errorf("should have failed with error %v (%s)", test.expectedError, test.description)
		}
	}

}

const RDBFile1 string = "REDIS0006\xfe\x00\x00\x03b_1\x04kuku\x00\x03a_1\x04lala\x00\x03b_3\xc3\t@\xb3\x01aa\xe0\xa6\x00\x01aa\xfc\xdb\x82\xb0\\B\x01\x00\x00\x00\x03b_2\r2343545345345\x00\x03a_2\xc0!\xffT\x81\xe9\x86\xcc\x9f\x1f\xc4"
const RDBFile2 string = "REDIS0001\xfe\x00\x00\ncompressed\xc3\x0c(\x04abcda\xe0\x18\x03\x01cd\x03\x05testz\x06\x01b\x0256\x01c\x0257\x03aaa\x0277\x04dddd\x011\x01a\x0243\x02aa\x017\xfe\x06\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\xfe\x07\x00\x12v3fe_Eramu@qik.com\xc0\x07\x00*v0a0_Ugrizmo4552d32c-af1e-484c-9d0b-6e4447\xc0\x04\xfe\x08\x00\x15v8da_Enikolay@qik.com\xc0\x08\x01\tbi_webapp\x06@q{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664050.7045,\"actor_id\":159973,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664056.18088,\"actor_id\":159974,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664560.31115,\"actor_id\":159975,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664565.91616,\"actor_id\":159976,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664820.23724,\"actor_id\":159977,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664860.49914,\"actor_id\":159978,\"ip\":null,\"app\":\"mob\"}\x00*v7c5_Ugrizmo15919895-bba1-47ef-8bdb-f6f968\xc0\x06\x00\x0bv693_dudeid\xc0\x08\xfe\t\x00*vd06_Ugrizmo29b59262-d286-4ed5-b7bf-1566cf\xc0\x03\x00*vf9e_Ugrizmo6b035e25-02b2-44ee-8860-48bac5\xc0\x05\x00*veaf_Ugrizmo468defb9-dc99-4cf2-92e7-cdef05\xc0\x01\x00*vc12_Ugrizmo8b2858e9-d439-4726-bb8e-abdbd9\xc0\x02\xfe\x0b\x00\x06v035_5\xc2\xe9p\x02\x00\xfe\x0e\x04\x0cv9a5_U159946\x03\x05dirty\x010\x07clients\x04\x80\x02].\x05users\x04\x80\x02].\x04\x0cv94b_U159973\x01\x05dirty\x011\x04\x0cv94a_U159974\x01\x05dirty\x011\x04\x0cv948_U159976\x01\x05dirty\x011\x04\x0cv946_U159978\x01\x05dirty\x011\x04\x0cv947_U159977\x01\x05dirty\x011\x04\x0cv949_U159975\x01\x05dirty\x011\xfe\x0f\x02\nv588_um_45\x01 f427ecf81e3afe3f4037a629944aaea0\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\xff"
