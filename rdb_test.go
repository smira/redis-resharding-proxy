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
			expected:      "REDIS0006\xfe\x00\x00\x03a_1\x04lala\x00\x03a_2\xc0!\xff\xad}0`\xa6\xf4\xa1\xab" + strings.Repeat("\xff", 56),
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
			expectedError: io.ErrUnexpectedEOF,
			filter:        func(string) bool { return true },
		},
		{
			description: "9: Old RDB, many types, no filtering",
			rdb:         RDBFile2,
			expected:    RDBFile2,
			filter:      func(string) bool { return true },
		},
		{
			description: "10: Old RDB, many types, fully filtered out",
			rdb:         RDBFile2,
			expected:    "REDIS0001\xfe\x00\xfe\x06\xfe\x07\xfe\x08\xfe\t\xfe\x0b\xfe\x0e\xfe\x0f\xff" + strings.Repeat("\xff", 1546),
			filter:      func(string) bool { return false },
		},
		{
			description: "11: Old RDB, many types, some filtered out",
			rdb:         RDBFile2,
			expected:    "REDIS0001\xfe\x00\xfe\x06\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\xfe\x07\xfe\x08\xfe\t\xfe\x0b\xfe\x0e\xfe\x0f\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\xff" + strings.Repeat("\xff", 1358),
			filter:      func(key string) bool { return strings.HasPrefix(key, "v02") },
		},
		{
			description: "12: RDB with list",
			rdb:         RDBFile3,
			expected:    RDBFile3,
			filter:      func(key string) bool { return true },
		},
		{
			description: "13: RDB with integer keys",
			rdb:         RDBFile4,
			expected:    "REDIS0006\xfe\x00\x00\xc0\f\x03abc\x00\u0087\xd6\x12\x00\x03fgh\xffQ\a\xb5\t\xfb\xe8ɦ\xff\xff\xff\xff\xff\xff\xff\xff",
			filter:      func(key string) bool { return strings.HasPrefix(key, "1") },
		},
		{
			description: "14: RDB with lzf compressed strings",
			rdb:         RDBFile5,
			expected:    "REDIS0006\xfe\x00\x00\xc3\x12/\x01aa \x00\x00d\xe0\n\x00\x00e\xe0\n\x00\x01ee\x02x3\x00\xc3\x130\x01aa\xe0\a\x00\x00b\xe0\b\x00\x00c\xe0\x00\x00\x01cc\x02x1\xff\x8f\xa2\xae٠Y\xa8N\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
			filter:      func(key string) bool { return strings.HasPrefix(key, "aaaa") },
		},
	}

	for _, test := range tests {
		ch := make(chan []byte)
		hadError := false

		go func() {
			err := FilterRDB(bufio.NewReader(bytes.NewBufferString(test.rdb)), ch, test.filter, int64(len(test.rdb)))
			if err != nil {
				if test.expectedError == nil || test.expectedError != err {
					t.Errorf("Filtering failed (%s): %v", test.description, err)
				} else {
					hadError = true
				}

			}
			close(ch)
		}()

		received := ""

		for data := range ch {
			received += string(data)
		}

		if test.expected != "" && len(received) != len(test.rdb) {
			t.Errorf("Size of filtered RDB doesn't match original size: %d != %d (test %s)", len(received), len(test.rdb), test.description)
		}

		if test.expected != "" && test.expected != received {
			t.Errorf("output not equal to expected: %#v != %#v (test %s)", test.expected, received, test.description)
		}

		if test.expectedError != nil && !hadError {
			t.Errorf("should have failed with error %v (%s)", test.expectedError, test.description)
		}
	}

}

func runRDBBenchmark(b *testing.B, filter func(string) bool) {
	for i := 0; i < b.N; i++ {
		ch := make(chan []byte)

		go func() {
			err := FilterRDB(bufio.NewReader(bytes.NewBufferString(RDBFile2)), ch, filter, int64(len(RDBFile2)))
			close(ch)
			if err != nil {
				b.Fatalf("Unable to filter RDB: %v", err)
			}
		}()

		for _ = range ch {
		}

	}
}

func BenchmarkFilterRDBCopy(b *testing.B) {
	runRDBBenchmark(b, func(string) bool { return true })
}

func BenchmarkFilterRDBDiscard(b *testing.B) {
	runRDBBenchmark(b, func(string) bool { return false })
}

func BenchmarkFilterRDBSome(b *testing.B) {
	runRDBBenchmark(b, func(key string) bool { return strings.HasPrefix(key, "v02") })
}

const (
	RDBFile1 = "REDIS0006\xfe\x00\x00\x03b_1\x04kuku\x00\x03a_1\x04lala\x00\x03b_3\xc3\t@\xb3\x01aa\xe0\xa6\x00\x01aa\xfc\xdb\x82\xb0\\B\x01\x00\x00\x00\x03b_2\r2343545345345\x00\x03a_2\xc0!\xffT\x81\xe9\x86\xcc\x9f\x1f\xc4"
	RDBFile2 = "REDIS0001\xfe\x00\x00\ncompressed\xc3\x0c(\x04abcda\xe0\x18\x03\x01cd\x03\x05testz\x06\x01b\x0256\x01c\x0257\x03aaa\x0277\x04dddd\x011\x01a\x0243\x02aa\x017\xfe\x06\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\xfe\x07\x00\x12v3fe_Eramu@qik.com\xc0\x07\x00*v0a0_Ugrizmo4552d32c-af1e-484c-9d0b-6e4447\xc0\x04\xfe\x08\x00\x15v8da_Enikolay@qik.com\xc0\x08\x01\tbi_webapp\x06@q{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664050.7045,\"actor_id\":159973,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664056.18088,\"actor_id\":159974,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664560.31115,\"actor_id\":159975,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664565.91616,\"actor_id\":159976,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664820.23724,\"actor_id\":159977,\"ip\":null,\"app\":\"mob\"}@r{\"event\":\"webapp.user.signup\",\"method\":\"api\",\"timestamp\":1311664860.49914,\"actor_id\":159978,\"ip\":null,\"app\":\"mob\"}\x00*v7c5_Ugrizmo15919895-bba1-47ef-8bdb-f6f968\xc0\x06\x00\x0bv693_dudeid\xc0\x08\xfe\t\x00*vd06_Ugrizmo29b59262-d286-4ed5-b7bf-1566cf\xc0\x03\x00*vf9e_Ugrizmo6b035e25-02b2-44ee-8860-48bac5\xc0\x05\x00*veaf_Ugrizmo468defb9-dc99-4cf2-92e7-cdef05\xc0\x01\x00*vc12_Ugrizmo8b2858e9-d439-4726-bb8e-abdbd9\xc0\x02\xfe\x0b\x00\x06v035_5\xc2\xe9p\x02\x00\xfe\x0e\x04\x0cv9a5_U159946\x03\x05dirty\x010\x07clients\x04\x80\x02].\x05users\x04\x80\x02].\x04\x0cv94b_U159973\x01\x05dirty\x011\x04\x0cv94a_U159974\x01\x05dirty\x011\x04\x0cv948_U159976\x01\x05dirty\x011\x04\x0cv946_U159978\x01\x05dirty\x011\x04\x0cv947_U159977\x01\x05dirty\x011\x04\x0cv949_U159975\x01\x05dirty\x011\xfe\x0f\x02\nv588_um_45\x01 f427ecf81e3afe3f4037a629944aaea0\x02\x0bv02e_um_108\x01 86756ab85811f6603e59c6d5911c858c\x02\x0bv02d_um_109\x01 86756ab85811f6603e59c6d5911c858c\xff"
	RDBFile3 = "REDIS0006\xfe\x00\n\x06mylist\xc3A\xbeE\x83\x04\x83\x05\x00\x00t \x03\x04d\x00\x00\x0c0\xe0\x00\x00\x0270\x0e\xe0\x02\r\x0115\xe0\x03\r\x0124\xe0\x03\r\x0198\xe0\x03\r\x0137\xe0\x03\r\x008\xe0\x04)\x0119\xe0\x03\x1b\x0121\xe0\x03\r\x0173\xe0\x03\r\x002\xe0\x04)\x0142\xe0\x03\x1b\x003\xe0\x04\x1b\x009\xe0\x04a\x0186\xe0\x03)\x002\xe0\x04\r\x001\xe0\x12E\x006\xe0\x04\xc3\x007\xe0\x047\x006\xe1\x04\t\x003\xe0\x04E\x009\xe0\x04\x8b\x005\xe0\x04\x8b\x005\xe0\x04\xdf\x000\xe0\x04\xdf\x001\xe0\x04\x1b\xe1\x05%\x008\xe0\x05\x8b\xe0\x05\r\xe0\x04\x99\x000\xe0\x04\x1b\x008\xe1\x04y\x005\xe0\x04\xb5\x004\xe0\x04}\x006\xe0\x04\xa7\x003\xe0\x04\r\x006\xe0\x04E\x001\xe0\x04\x1b\x004\xe2\x04\x05\x005\xe0\x04\x8b\x008\xe1\x05\x17\xe0\x04)\xe2\x05/\x005\xe0\x05a\xe0\x04\x99\xe1\x06\xf7\xe0\x04a\x000\xe0\x04S\x003\xe0\x04\x1b\x002\xe0\x04S\xe1\x05O\x002\xe0\x047\x009\xe0\x04o\xe0\x05S\x008\xe1\x04\x95\x009\xe0\x04a\xe3\x05\x0f\x006\xe0\x04\xed\xe2\x05\xbb\xe1\x06\x17\xe1\x04\x87\x009\xe0\x04a\xe2\x06\x9f\xe0\x04\x99\xe1\x05\x87\x006\xe2\x05g\xe1\x04\xf7\x002\xe0\x04\x8b\x000\xe0\x04\r\xe1\x05O\x003\xe1\x04\t\xe0\x05a\x002\xe0\x05}\xe0\x04E\x003\xe0\x04\xc3\xe1\x05\xe9\xe0\x05a\xe0\x05S\xe3\x05U\xe2\x05\xf3\xe1\x05\xbf\x007\xe0\x04o\xe1\x05\x17\x004\xe0\x04}\xe0\x05\x1b\x006\xe0\x04\xb5\x005\xe1\x04%\x009\xe0\x057\xe2\x04!\xe0\x05\xdf\xe4\x05\x89\x004\xe0\x05E\xe0\x04\x99\xe4\x05_\xe1\x05O\xe2\x05Y\xe5\x051\x007\xe0\x04}\xe0\x05E\x0283\xff\xffy\xaa\x8e\x05\xb8\xd6\xecX"
	RDBFile4 = "REDIS0006\xfe\x00\x00\xc1aS\x03cde\x00\xc0\x0c\x03abc\x00\xc2\x87\xd6\x12\x00\x03fgh\xff\xe9 \xb4\xe35e\x99\x92"
	RDBFile5 = "REDIS0006\xfe\x00\x00\xc3\x12/\x01aa \x00\x00d\xe0\n\x00\x00e\xe0\n\x00\x01ee\x02x3\x00\xc3\x120\x01bb\xe0\x07\x00\x00a\xe0\t\x00\x00c\xc0\x00\x01cc\x02x2\x00\xc3\x130\x01aa\xe0\x07\x00\x00b\xe0\x08\x00\x00c\xe0\x00\x00\x01cc\x02x1\xff\x83J\xb9\xf9mX\x8a\xa6"
)
