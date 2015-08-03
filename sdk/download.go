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
)

type chunksDownload struct {
	mc        []metaChunk
	closed    bool
	nextIdx   int
	currentIn *metaChunkReader
}

func makeChunksDownload(chunks []Chunk) (*chunksDownload, error) {
	var err error
	cd := new(chunksDownload)
	cd.mc, err = organizeChunks(chunks)
	cd.closed = false
	cd.nextIdx = 0
	cd.currentIn = nil
	if err != nil {
		return nil, err
	}
	return cd, nil
}

func (dl *chunksDownload) Close() error {
	if dl.closed {
		return io.ErrClosedPipe
	}
	if dl.currentIn != nil {
		dl.currentIn.Close()
		dl.currentIn = nil
	}
	dl.closed = true
	return nil
}

func (dl *chunksDownload) Read(p []byte) (int, error) {
	var err error

	if dl.closed {
		return 0, io.ErrClosedPipe
	}

	if dl.currentIn == nil {
		if dl.nextIdx >= len(dl.mc) {
			return 0, io.EOF
		}
		dl.currentIn, err = newMetaChunkReader(dl.mc[dl.nextIdx])
		if err != nil {
			return 0, err
		}
		dl.nextIdx++
	}

	var n int
	n, err = dl.currentIn.Read(p)
	if err == nil {
		return n, nil
	}
	if err == io.EOF {
		dl.currentIn.Close()
		dl.currentIn = nil
		if n > 0 {
			return n, nil
		} else {
			return dl.Read(p) // XXX recurse with the next
		}
	}

	return 0, err
}
