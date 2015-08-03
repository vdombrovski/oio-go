// OpenIO SDS Go client SDK
// Copyright (C) 2015 OpenIO
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 3.0 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library.

package oio

import (
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type metaChunkReader struct {
	mc     metaChunk
	closed bool

	client http.Client
	resp   *http.Response
}

type metaChunk struct {
	offset    uint64
	meta_size uint64
	data      []Chunk
	parity    []Chunk
}

type position struct {
	idx    int
	meta   int
	intra  int
	parity bool
}

type positionSet struct {
	tab []position
}

func (s *positionSet) Len() int {
	return len(s.tab)
}

func (s *positionSet) Less(i, j int) bool {
	if s.tab[i].meta == s.tab[j].meta {
		if s.tab[i].intra == s.tab[j].intra {
			return s.tab[i].parity && !s.tab[j].parity
		}
		return s.tab[i].intra < s.tab[j].intra
	}
	return s.tab[i].meta < s.tab[j].meta
}

func (s *positionSet) Swap(i, j int) {
	var tmp position
	tmp = s.tab[i]
	s.tab[i] = s.tab[j]
	s.tab[j] = tmp
}

func organizeChunks(chunks []Chunk) ([]metaChunk, error) {
	pos := positionSet{tab: make([]position, len(chunks), len(chunks))}

	// extract the position of all the chunks
	for idx, chunk := range chunks {
		pos.tab[idx] = (&chunk).getPositon()
		pos.tab[idx].idx = idx
	}

	// sort the chunks by position by ascending meta2, intra, then parity
	sort.Sort(&pos)

	// now organize the chunks into set serving the same meta chunk
	out := make([]metaChunk, 0)
	var current_meta metaChunk
	var current_idx int = 0
	append_and_renew := func() {
		out = append(out, current_meta)
		current_meta.data = make([]Chunk, 0)
		current_meta.parity = make([]Chunk, 0)
	}
	for i := 0; i < len(pos.tab); i = i + 1 {
		if pos.tab[i].meta != current_idx { // renew the metaChunk
			append_and_renew()
			current_idx = pos.tab[i].meta
		}
		if pos.tab[i].parity {
			current_meta.parity = append(current_meta.parity, chunks[pos.tab[i].idx])
		} else {
			current_meta.data = append(current_meta.data, chunks[pos.tab[i].idx])
		}
	}
	if len(current_meta.data) > 0 {
		append_and_renew()
	}
	return out, nil
}

func (chunk *Chunk) getPositon() position {
	var p position
	tokens := strings.SplitN(chunk.Position, ".", 2)
	p.meta, _ = strconv.Atoi(tokens[0])
	if len(tokens) > 1 {
		p.intra, _ = strconv.Atoi(tokens[1])
		if strings.HasSuffix(tokens[1], "p") {
			p.parity = true
		}
	}
	return p
}

func maxSize(tab *[]Chunk) uint64 {
	var v uint64 = 0
	for _, c := range *tab {
		if c.Size > v {
			v = c.Size
		}
	}
	return v
}

func decRet(base *uint64, op uint64) uint64 {
	if *base < op {
		tmp := *base
		*base = 0
		return tmp
	} else {
		*base = *base - op
		return op
	}
}

func newMetaChunkReader(mc metaChunk) (*metaChunkReader, error) {
	mcr := new(metaChunkReader)
	mcr.mc = mc
	dial := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 1000*time.Millisecond)
	}
	transport := http.Transport{Dial: dial}
	mcr.client = http.Client{Transport: &transport}
	if err := mcr.open(); err != nil {
		return nil, err
	}
	return mcr, nil
}

func (mcr *metaChunkReader) open() error {
	if mcr.closed {
		return io.ErrClosedPipe
	}

	var err error
	mcr.resp, err = mcr.client.Get(mcr.mc.data[0].Url)
	return err
}

func (mcr *metaChunkReader) Close() error {
	if mcr.closed {
		return io.ErrClosedPipe
	}
	if mcr.resp != nil {
		mcr.resp.Body.Close()
		mcr.resp = nil
	}
	mcr.closed = true
	return nil
}

func (mcr *metaChunkReader) Read(p []byte) (int, error) {

	if mcr.closed {
		return 0, io.ErrClosedPipe
	}

	if mcr.resp == nil {
		panic("metaChunkReader not initialized")
	}

	n, err := mcr.resp.Body.Read(p)
	if err == nil {
		return n, nil
	}

	return n, err
}
