// OpenIO SDS Go rawx
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
// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see <http://www.gnu.org/licenses/>.

package main

/*
Serves a BLOB repository via a HTTP server.
@TODO implement compression with any of the compress.* modules
*/

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

func setErrorString(rep http.ResponseWriter, s string) {
	rep.Header().Set("X-Error", s)
}

func setError(rep http.ResponseWriter, e error) {
	setErrorString(rep, e.Error())
}

type rawxService struct {
	ns   string
	url  string
	repo Repository
	compress bool
}

type attrMapping struct {
	attr string
	header string
}

const bufSize = 16384

var (
	HeaderPrefix string = "X-Oio-"
	AttrPrefix = "user.grid."
)

var (
	AttrNameCompression = "compression"
	AttrNameChecksum = "chunk.hash"
)

var (
	AttrValueZLib []byte = []byte("zlib")
)

var (
	ErrCompressionNotManaged = errors.New("Compression mode not managed")
	ErrMissingHeader = errors.New("Missing mandatory header")
	ErrMd5Mismatch = errors.New("MD5 sum mismatch")
)

var AttrMap []attrMapping = []attrMapping{
	{"container.id", "Chunk-Meta-Container-Id"},
	{"content.path", "Chunk-Meta-Content-Path"},
	{"content.size", "Chunk-Meta-Content-Size"},
	{"content.ver",  "Chunk-Meta-Content-Path"},
	{"chunk.nb",     "Chunk-Meta-Content-Version"},
	{"chunk.id",     "Chunk-Meta-Chunk-Id"},
	{"chunk.size",   "Chunk-Meta-Chunk-Size"},
	{"chunk.pos",    "Chunk-Meta-Chunk-Pos"},
}

type upload struct {
	in io.Reader
	length int64
	h string
}

func putData(out io.Writer, ul *upload) error {
	remaining := ul.length
	chunkHash := md5.New()
	buf := make([]byte, bufSize)
	for remaining > 0 {
		max := remaining
		if max > bufSize {
			max = bufSize
		}
		n, err := ul.in.Read(buf[:max])
		if n > 0 {
			out.Write(buf[:n])
			chunkHash.Write(buf[:n])
			remaining = remaining - int64(n)
		}
		if err != nil {
			return err
		}
	}

	sum := chunkHash.Sum(make([]byte,0))
	ul.h = strings.ToUpper(hex.EncodeToString(sum))
	return nil
}

func putFinish(out FileWriter, req *http.Request, h string) error {

	// If a hash has been sent, it must match the hash computed
	if h0 := req.Header.Get("chunkhash"); h0 != "" {
		if h != strings.ToUpper(h0) {
			return ErrMd5Mismatch
		}
	}

	// Set the xattr coming from the request
	for _, pair := range AttrMap {
		v := req.Header.Get(HeaderPrefix + pair.header)
		if v == "" {
			return ErrMissingHeader
		}
		if err := out.SetAttr(AttrPrefix+pair.attr, []byte(v)); err != nil {
			return err
		}
	}

	// Set the MD5
	out.SetAttr(AttrPrefix + AttrNameChecksum, []byte(h))

	return nil
}

func (self *rawxService) doPut(rep http.ResponseWriter, req *http.Request) {
	// Attempt a PUT in the repository
	out, err := self.repo.Put(req.URL.Path)
	if err != nil {
		if os.IsExist(err) {
			rep.WriteHeader(http.StatusForbidden)
		} else if err == os.ErrInvalid {
			setError(rep, err)
			rep.WriteHeader(http.StatusBadRequest)
		} else {
			setError(rep, err)
			rep.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	// Upload, and maybe manage compression
	var ul upload
	ul.in = req.Body
	ul.length = req.ContentLength

	if self.compress {
		z := zlib.NewWriter(out)
		err = putData(z, &ul)
		errClose := z.Close()
		if err == nil {
			err = errClose
		}
	} else {
		err = putData(out, &ul)
	}

	// Finish with the XATTR management 
	if err != nil {
		err = putFinish(out, req, ul.h)
	}

	// Then reply
	if err != nil {
		setError(rep, err)
		rep.WriteHeader(http.StatusInternalServerError)
		out.Abort()
	} else {
		out.Commit()
		rep.Header().Set("chunkhash", ul.h)
		rep.WriteHeader(http.StatusOK)
	}
}

func (self *rawxService) doGetStats(rep http.ResponseWriter, req *http.Request) {
	allCounters := counters.Get()
	allTimers := timers.Get()

	rep.WriteHeader(200)
	for i,n := range names {
		rep.Write([]byte(fmt.Sprintf("timer.%s %v", n, allTimers[i])))
		rep.Write([]byte(fmt.Sprintf("counter.%s %v", n, allCounters[i])))
	}
}

func (self *rawxService) doGetChunk(rep http.ResponseWriter, req *http.Request) {
	inChunk, err := self.repo.Get(req.URL.Path)
	if inChunk != nil {
		defer inChunk.Close()
	}
	if err != nil {
		setError(rep, err)
		rep.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Check if there is some compression
	var v []byte
	var in io.ReadCloser
	v, err = inChunk.GetAttr(AttrPrefix + AttrNameCompression)
	if err != nil {
		in = ioutil.NopCloser(inChunk)
		err = nil
	} else if bytes.Equal(v, AttrValueZLib) {
		in, err = zlib.NewReader(in)
	} else {
		in = nil
		err = ErrCompressionNotManaged
	}
	if in != nil {
		defer in.Close()
	}
	if err != nil {
		setError(rep, err)
		rep.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Now transmit the clear data to the client
	length := inChunk.Size()
	for _, pair := range AttrMap {
		v, err := inChunk.GetAttr(AttrPrefix + pair.attr)
		if err != nil {
			rep.Header().Set(pair.header, string(v))
		}
	}
	rep.Header().Set("Content-Length", fmt.Sprintf("%v", length))
	rep.WriteHeader(200)
	buf := make([]byte, bufSize)
	for {
		n, err := in.Read(buf)
		if n > 0 {
			rep.Write(buf[:n])
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("Write() error : %v", err)
			}
			break
		}
	}
}

func (self *rawxService) doDel(rep http.ResponseWriter, req *http.Request) {
	if err := self.repo.Del(req.URL.Path); err != nil {
		if os.IsNotExist(err) {
			rep.WriteHeader(http.StatusNotFound)
		} else {
			setError(rep, err)
			rep.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		rep.WriteHeader(200)
	}
}

// Makes the blobHttpHandler a http.Handler
func (self *rawxService) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	var which int = Unexpected
	if req.Method == "PUT" {
		which = StatSlotPut
		self.doPut(rep, req)
	} else if req.Method == "GET" {
		if req.URL.Path == "/stat" {
			which = StatSlotGetStats
			self.doGetStats(rep, req)
		} else {
			which = StatSlotGet
			self.doGetChunk(rep, req)
		}
	} else if req.Method == "DELETE" {
		which = StatSlotDel
		self.doDel(rep, req)
	} else {
		setErrorString(rep, "only PUT,GET,DELETE")
		rep.WriteHeader(http.StatusMethodNotAllowed)
	}
	counters.Increment(which)
	log.Println("ACCESS", req)
}

