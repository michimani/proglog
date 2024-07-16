package log_test

import (
	"os"
	"testing"

	"github.com/michimani/proglog/internal/log"
	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + log.Exported_lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	rqir := require.New(t)

	f, err := os.CreateTemp("", "store_append_read_test")
	rqir.NoError(err)

	defer os.Remove(f.Name())
	s, err := log.Exported_newStore(f)
	rqir.NoError(err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = log.Exported_newStore(f)
	rqir.NoError(err)
	testRead(t, s)
}

// helper

func testAppend(t *testing.T, s *log.Exported_store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)

	}
}

func testRead(t *testing.T, s *log.Exported_store) {
	t.Helper()
	var pos uint64

	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *log.Exported_store) {
	t.Helper()

	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, log.Exported_lenWidth)
		n, err := s.ReadAt(b, off)

		require.NoError(t, err)
		require.Equal(t, log.Exported_lenWidth, n)
		off += int64(n)

		size := log.Exported_enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}
