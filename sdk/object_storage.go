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
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
)

var (
	errNoContentId = errors.New("Missing Content-Id")
	errInvalidVersion = errors.New("No version received from the proxy")
	errECNotImplemented = errors.New("EC Not implemented")
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
	content, err := cli.container.GetContent(n)
	if err != nil {
		return nil, err
	}

	// Only consider the HTTP locations, no other tier is managed yet
	rawx_chunks := make([]Chunk,0)
	for _, chunk := range content.Chunks {
		if strings.HasPrefix(chunk.Url, "http://") {
			rawx_chunks = append(rawx_chunks, chunk)
		}
	}
	content.Chunks = rawx_chunks

	return makeChunksDownload(content.Chunks)
}

func (cli *objectStorageClient) PutContent(n ObjectName, size uint64, auto bool, src io.ReadSeeker) error {
	if src == nil {
		panic("Invalid input")
	}


	var err error

	content, err := cli.container.GenerateContent(n, size, auto)
	if err != nil {
		return err
	}
	mcSet, err := organizeChunks(content.Chunks)
	if err != nil {
		return err
	}

	// If an explicit Id has been provided, it must supersede the ID
	// generated by the proxy
	id := n.Id()
	if len(id) == 0 {
		id = content.Header.Id
		if len(id) == 0 {
			return errNoContentId
		}
	}

	// Idem, an explicit version is stronger than the proxy-generated version
	// number
	v := n.Version()
	if v == 0 {
		v = content.Header.Version
		if v == 0 {
			return errInvalidVersion
		}
	}

	// Alert if RAIN is used (NYI)
	for _, mc := range mcSet {
		if len(mc.parity) > 0 {
			return errECNotImplemented
		}
	}

	// Patch the chunks'es size
	// TODO get the chunk_size from somewhere reliable (i.e. the config)
	var offset uint64 = 0
	chunk_size := maxSize(&content.Chunks)
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

	cid := strings.ToUpper(hex.EncodeToString(ComputeUserId(n)))

	// upload each meta-chunk
	for i, _ := range mcSet {
		mc := &(mcSet[i])
		pp := makePolyPut()
		for _, chunk := range content.Chunks {
			pp.addTarget(chunk.Url)
		}
		pp.addHeader("X-oio-req-id", "0")
		pp.addHeader(RAWX_HEADER_PREFIX+"container-id", cid)
		pp.addHeader(RAWX_HEADER_PREFIX+"content-path", n.Path())
		pp.addHeader(RAWX_HEADER_PREFIX+"content-id", id)
		pp.addHeader(RAWX_HEADER_PREFIX+"content-version", strconv.FormatUint(v, 10))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-size", strconv.FormatUint(size, 10))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-chunksnb", strconv.Itoa(len(mcSet)))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-storage-policy", content.Header.Policy)
		pp.addHeader(RAWX_HEADER_PREFIX+"content-chunk-method", content.Header.ChunkMethod)
		pp.addHeader(RAWX_HEADER_PREFIX+"content-mime-type", content.Header.MimeType)
		pp.addHeader(RAWX_HEADER_PREFIX+"chunk-pos", strconv.Itoa(i))
		// the chunk-id is set by the "polyput" itself, because it varies
		// for each chunk
		r := makeSliceReader(src, mc.meta_size)
		err = pp.do(&r)
		if err != nil {
			return err
		}
	}

	err = cli.container.PutContent(n, content, auto)
	if err != nil {
		return err
	}
	return nil
}
