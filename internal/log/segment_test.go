package log_test

import (
	"io"
	"os"
	"testing"

	api "github.com/michimani/proglog/api/v1"
	"github.com/michimani/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSegment(t *testing.T) {
	rqir := require.New(t)

	dir, _ := os.MkdirTemp("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := log.Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = log.Exported_entWidth * 3

	s, err := log.Exported_newSegment(dir, 16, c)
	rqir.NoError(err)
	rqir.Equal(uint64(16), s.Exported_NextOffset(), s)
	rqir.False(s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		rqir.NoError(err)
		rqir.Equal(16+i, off)

		got, err := s.Read(off)
		rqir.NoError(err)
		rqir.Equal(want.Value, got.Value)
	}

	_, err = s.Append(want)
	rqir.Equal(io.EOF, err)

	rqir.True(s.IsMaxed())
	rqir.NoError(s.Close())

	p, _ := proto.Marshal(want)
	c.Segment.MaxStoreBytes = uint64(len(p)+log.Exported_lenWidth) * 4
	c.Segment.MaxIndexBytes = 1024

	s, err = log.Exported_newSegment(dir, 16, c)
	rqir.NoError(err)

	rqir.True(s.IsMaxed())

	rqir.NoError(s.Remove())

	s, err = log.Exported_newSegment(dir, 16, c)
	rqir.NoError(err)
	rqir.False(s.IsMaxed())
	rqir.NoError(s.Close())
}
