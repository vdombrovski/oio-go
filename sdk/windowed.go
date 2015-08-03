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

type sliceReader struct {
	in        io.ReadSeeker
	original  uint64
	remaining uint64
	closed    bool
}

func makeSliceReader(src io.ReadSeeker, size uint64) sliceReader {
	return sliceReader{
		in:        src,
		original:  size,
		remaining: size,
		closed:    false,
	}
}

func (r *sliceReader) Len() int64 {
	return int64(r.original)
}

func (r *sliceReader) Close() error {
	if r.remaining <= 0 {
		return io.EOF
	}
	r.remaining = 0
	return nil
}

func (r *sliceReader) Read(buf []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	size := len(buf)
	if uint64(size) > r.remaining {
		size = int(r.remaining)
	}
	if n, err := r.in.Read(buf[:size]); err != nil {
		if err == io.EOF {
			return n, io.ErrUnexpectedEOF
		} else {
			return n, err
		}
	} else {
		r.remaining = r.remaining - uint64(n)
		return n, nil
	}
}
