package log_test

import (
	"io"
	"os"
	"testing"

	"github.com/michimani/proglog/internal/log"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	rqir := require.New(t)

	f, err := os.CreateTemp(os.TempDir(), "index_test")
	rqir.NoError(err)
	defer os.Remove(f.Name())

	c := log.Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := log.Exported_newIndex(f, c)
	rqir.NoError(err)

	_, _, err = idx.Read(-1)
	rqir.Error(err)
	rqir.Equal(f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		rqir.NoError(err)

		_, pos, err := idx.Read(int64(want.Off))
		rqir.NoError(err)
		rqir.Equal(want.Pos, pos)

	}
	_, _, err = idx.Read(int64(len(entries)))
	rqir.Equal(io.EOF, err)
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = log.Exported_newIndex(f, c)
	rqir.NoError(err)

	off, pos, err := idx.Read(-1)
	rqir.NoError(err)
	rqir.Equal(uint32(1), off)
	rqir.Equal(entries[1].Pos, pos)
}
