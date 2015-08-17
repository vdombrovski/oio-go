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
	"errors"
	"io"
	"strconv"
)

type objectStorageClient struct {
	directory Directory
	container Container
}

func (cli *objectStorageClient) DeleteContent(n ObjectName) error {
	_, err := cli.container.DeleteContent(n)
	return err
}

func (cli *objectStorageClient) GetContent(n ObjectName) (io.ReadCloser, error) {
	chunks, _, err := cli.container.GetContent(n)
	if err != nil {
		return nil, err
	}
	return makeChunksDownload(chunks)
}

func (cli *objectStorageClient) PutContent(n ObjectName, size uint64, src io.ReadSeeker) error {
	if src == nil {
		panic("Invalid input")
	}
	var err error

	// Get a list of chunks
	chunks, err := cli.container.GenerateContent(n, size)
	if err != nil {
		return err
	}

	mcSet, err := organizeChunks(chunks)
	if err != nil {
		return err
	}
	// Alert if RAIN is used (NYI)
	for _, mc := range mcSet {
		if len(mc.parity) > 0 {
			return errors.New("Erasure coding not yet implemented")
		}
	}

	// Patch the chunks'es size
	// TODO get the chunk_size from somewhere reliable (i.e. the config)
	var offset uint64 = 0
	chunk_size := maxSize(&chunks)
	remaining := size
	for i, _ := range mcSet {
		mc := &(mcSet[i])
		mc.meta_size = decRet(&remaining, chunk_size)
		mc.offset = offset
		offset = offset + mc.meta_size
		for ii, _ := range (*mc).data {
			(*mc).data[ii].Size = mc.meta_size
		}
	}

	// upload each meta-chunk
	for i, _ := range mcSet {
		mc := &(mcSet[i])
		pp := makePolyPut()
		for _, chunk := range chunks {
			pp.addTarget(chunk.Url)
		}
		pp.addHeader("X-oio-req-id", "0")
		pp.addHeader(RAWX_HEADER_PREFIX+"container-id", "0000000000000000000000000000000000000000000000000000000000000000")
		pp.addHeader(RAWX_HEADER_PREFIX+"content-path", n.Path())
		pp.addHeader(RAWX_HEADER_PREFIX+"content-size", strconv.FormatUint(size, 10))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-chunksnb", strconv.Itoa(len(mcSet)))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-metadata-sys", "")
		pp.addHeader(RAWX_HEADER_PREFIX+"chunk-id", "0000000000000000000000000000000000000000000000000000000000000000")
		pp.addHeader(RAWX_HEADER_PREFIX+"chunk-pos", strconv.Itoa(i))
		r := makeSliceReader(src, mc.meta_size)
		err = pp.do(&r)
		if err != nil {
			return err
		}
	}

	err = cli.container.PutContent(n, size, chunks)
	if err != nil {
		return err
	}
	return nil
}
