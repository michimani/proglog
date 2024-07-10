package server_test

import (
	"sync"
	"testing"

	"github.com/michimani/proglog/internal/server"
	"github.com/stretchr/testify/assert"
)

func Test_Log_Append(t *testing.T) {
	cases := []struct {
		name      string
		l         *server.Log
		rl        []server.Record
		expect    []server.Record
		wantError bool
	}{
		{
			name: "single record",
			l:    server.NewLog(),
			rl: []server.Record{
				{Value: []byte("hello")},
			},
			expect: []server.Record{
				{Value: []byte("hello"), Offset: 0},
			},
		},
		{
			name: "multiple records",
			l:    server.NewLog(),
			rl: []server.Record{
				{Value: []byte("hello")},
				{Value: []byte("world")},
			},
			expect: []server.Record{
				{Value: []byte("hello"), Offset: 0},
				{Value: []byte("world"), Offset: 1},
			},
		},
		{
			name: "nil log",
			l:    nil,
			rl: []server.Record{
				{Value: []byte("hello")},
			},
			expect:    nil,
			wantError: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			asst := assert.New(tt)

			for _, r := range c.rl {
				_, err := c.l.Append(r)
				if c.wantError {
					asst.Error(err)
					return
				}

				asst.NoError(err)
			}

			arl := c.l.Exported_records()
			asst.Equal(c.expect, arl)
		})
	}
}

func Test_Log_Append_RaceCondition(t *testing.T) {
	cases := []struct {
		name      string
		l         *server.Log
		rl        []server.Record
		expectLen int
	}{
		{
			name: "single record",
			l:    server.NewLog(),
			rl: []server.Record{
				{Value: []byte("hello")},
			},
			expectLen: 1,
		},
		{
			name: "multiple records",
			l:    server.NewLog(),
			rl: []server.Record{
				{Value: []byte("message 1")},
				{Value: []byte("message 2")},
				{Value: []byte("message 3")},
				{Value: []byte("message 4")},
				{Value: []byte("message 5")},
			},
			expectLen: 5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			asst := assert.New(tt)

			var wg sync.WaitGroup
			for _, r := range c.rl {
				wg.Add(1)
				go func(record server.Record) {
					defer wg.Done()
					_, err := c.l.Append(record)
					asst.NoError(err)
				}(r)
			}
			wg.Wait()

			arl := c.l.Exported_records()
			asst.Equal(c.expectLen, len(arl))
		})
	}
}

func Test_Log_Read(t *testing.T) {
	l := server.NewLog()
	l.Exported_setRecords([]server.Record{
		{Value: []byte("hello"), Offset: 0},
		{Value: []byte("world"), Offset: 1},
		{Value: []byte("thank you"), Offset: 2},
	})

	cases := []struct {
		name      string
		l         *server.Log
		offset    uint64
		expect    server.Record
		wantError bool
	}{
		{
			name:   "first record",
			l:      l,
			offset: 0,
			expect: server.Record{Value: []byte("hello"), Offset: 0},
		},
		{
			name:   "second record",
			l:      l,
			offset: 1,
			expect: server.Record{Value: []byte("world"), Offset: 1},
		},
		{
			name:   "third record",
			l:      l,
			offset: 2,
			expect: server.Record{Value: []byte("thank you"), Offset: 2},
		},
		{
			name:      "offset not found",
			l:         l,
			offset:    3,
			expect:    server.Record{},
			wantError: true,
		},
		{
			name:      "nil log",
			l:         nil,
			offset:    0,
			expect:    server.Record{},
			wantError: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			asst := assert.New(tt)

			r, err := c.l.Read(c.offset)
			if c.wantError {
				asst.Error(err)
				asst.Equal(r, server.Record{})
				return
			}

			asst.NoError(err)
			asst.Equal(c.expect, r)
		})
	}
}

func Test_Log_Read_RaceCondition(t *testing.T) {
	l := server.NewLog()
	l.Exported_setRecords([]server.Record{
		{Value: []byte("message 1"), Offset: 0},
		{Value: []byte("message 2"), Offset: 1},
		{Value: []byte("message 3"), Offset: 2},
		{Value: []byte("message 4"), Offset: 3},
		{Value: []byte("message 5"), Offset: 4},
	})

	cases := []struct {
		name       string
		offsetList []uint64
		expect     []server.Record
	}{
		{
			name:       "single record",
			offsetList: []uint64{0},
			expect: []server.Record{
				{Value: []byte("message 1"), Offset: 0},
			},
		},
		{
			name:       "multiple records",
			offsetList: []uint64{0, 1, 2, 3, 4},
			expect: []server.Record{
				{Value: []byte("message 1"), Offset: 0},
				{Value: []byte("message 2"), Offset: 1},
				{Value: []byte("message 3"), Offset: 2},
				{Value: []byte("message 4"), Offset: 3},
				{Value: []byte("message 5"), Offset: 4},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(tt *testing.T) {
			asst := assert.New(tt)

			var wg sync.WaitGroup
			for i, o := range c.offsetList {
				wg.Add(1)
				go func(offset uint64, expect server.Record) {
					defer wg.Done()
					r, err := l.Read(offset)
					asst.NoError(err)
					asst.Equal(expect, r)
				}(o, c.expect[i])
			}
			wg.Wait()
		})
	}
}
