// OpenIO SDS Go rawx
// Copyright (C) 2015-2018 OpenIO SAS
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
	"log/syslog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func setErrorString(rep http.ResponseWriter, s string) {
	rep.Header().Set("X-Error", s)
}

func setError(rep http.ResponseWriter, e error) {
	setErrorString(rep, e.Error())
}

type rawxService struct {
	ns       string
	url      string
	repo     Repository
	compress bool
}

type chunkHandler struct {
	rawx *rawxService
}

type chunkRequest struct {
	rawx    *rawxService
	req     *http.Request
	chunkid string
	reqid   string
  status int
  time_spent uint64
  bytes_out  uint64
}

type attrMapping struct {
	attr   string
	header string
}

const bufSize = 16384

var (
	HeaderPrefix string = "X-Oio-"
	AttrPrefix   string = "user.grid."
)

var (
	AttrNameCompression = "compression"
	AttrNameChecksum    = "chunk.hash"
)

var (
	AttrValueZLib []byte = []byte("zlib")
)

var (
	ErrChunkExists           = errors.New("Chunk already exists")
	ErrInvalidChunkName      = errors.New("Invalid chunk name")
	ErrCompressionNotManaged = errors.New("Compression mode not managed")
	ErrMissingHeader         = errors.New("Missing mandatory header")
	ErrMd5Mismatch           = errors.New("MD5 sum mismatch")
)

var AttrMap []attrMapping = []attrMapping{
	{"container.id", "Chunk-Meta-Container-Id"},
	{"content.path", "Chunk-Meta-Content-Path"},
	{"content.id", "Chunk-Meta-Content-Id"},
	{"content.ver", "Chunk-Meta-Content-Version"},
	{"content.size", "Chunk-Meta-Content-Size"},
	{"content.nbchunk", "Chunk-Meta-Content-Chunknb"},
	{"content.storage_policy", "Chunk-Meta-Content-Storage-Policy"},
	{"content.mime_type", "Chunk-Meta-Content-Mime-Type"},
	{"content.chunk_method", "Chunk-Meta-Content-Chunk-Method"},
	{"chunk.id", "Chunk-Meta-Chunk-Id"},
	{"chunk.size", "Chunk-Meta-Chunk-Size"},
	{"chunk.position", "Chunk-Meta-Chunk-Pos"},
	{"chunk.hash", "Chunk-Meta-Chunk-Pos"},
}

type upload struct {
	in     io.Reader
	length int64
	h      string
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

	sum := chunkHash.Sum(make([]byte, 0))
	ul.h = strings.ToUpper(hex.EncodeToString(sum))
	return nil
}

func (self *chunkRequest) replyError(rep http.ResponseWriter, err error) {
	if os.IsExist(err) {
		self.status = http.StatusForbidden
	} else {
		setError(rep, err)
		if err == os.ErrInvalid {
			self.status = http.StatusBadRequest
		} else {
			switch err {
			case ErrInvalidChunkName:
				self.status = http.StatusBadRequest
			case ErrMissingHeader:
				self.status = http.StatusBadRequest
			default:
				self.status = http.StatusInternalServerError
			}
		}
	}

  rep.WriteHeader(self.status)
}

func (self *chunkRequest) putFinish(out FileWriter, h string) error {

	// If a hash has been sent, it must match the hash computed
	if h0 := self.req.Header.Get("chunkhash"); h0 != "" {
		if h != strings.ToUpper(h0) {
			return ErrMd5Mismatch
		}
	}

	// Set the xattr coming from the request
	for _, pair := range AttrMap {
		v := self.req.Header.Get(HeaderPrefix + pair.header)
		if v == "" {
			return ErrMissingHeader
		}
		if err := out.SetAttr(AttrPrefix+pair.attr, []byte(v)); err != nil {
			return err
		}
	}

	// Set the MD5
	out.SetAttr(AttrPrefix+AttrNameChecksum, []byte(h))

	return nil
}

func (self *chunkRequest) upload(rep http.ResponseWriter) {

	// Check all the mandatory headers are present

	// Attempt a PUT in the repository
	out, err := self.rawx.repo.Put(self.chunkid)
	if err != nil {
		self.replyError(rep, err)
		return
	}

	// Upload, and maybe manage compression
	var ul upload
	ul.in = self.req.Body
	ul.length = self.req.ContentLength

	if self.rawx.compress {
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
		err = self.putFinish(out, ul.h)
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

func (self *chunkRequest) download(rep http.ResponseWriter) {
	inChunk, err := self.rawx.repo.Get(self.chunkid)
	if inChunk != nil {
		defer inChunk.Close()
	}
	if err != nil {
		self.replyError(rep, err)
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
      self.bytes_out = self.bytes_out + uint64(n)
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

func (self *chunkRequest) removal(rep http.ResponseWriter) {
	if err := self.rawx.repo.Del(self.chunkid); err != nil {
		self.replyError(rep, err)
	} else {
		rep.WriteHeader(200)
	}
}

func (self *chunkHandler) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	var stats_hits, stats_time int
	pre := time.Now()

	// Extract some common headers
	reqid := req.Header.Get("X-oio-reqid")
	if len(reqid) <= 0 {
		reqid = req.Header.Get("X-trans-id")
	}
  if len(reqid) > 0 {
    rep.Header().Set("X-trans-id", reqid)
    rep.Header().Set("X-oio-reqid", reqid)
  }

	// Forward to the request method
	chunkreq := chunkRequest{ rawx: self.rawx, req: req,
		chunkid: filepath.Base(req.URL.Path), reqid: reqid,
	}

	switch req.Method {
	case "PUT":
		stats_time = TimePut
		stats_hits = HitsPut
		chunkreq.upload(rep)
	case "GET":
		stats_time = TimeGet
		stats_hits = HitsGet
		chunkreq.download(rep)
	case "DELETE":
		stats_time = TimeDel
		stats_hits = HitsDel
		chunkreq.removal(rep)
	default:
		stats_time = TimeOther
		stats_hits = HitsOther
		rep.WriteHeader(http.StatusMethodNotAllowed)
	}
	spent := uint64(time.Since(pre).Nanoseconds() / 1000)

	// Increment counters and log the request
	counters.Increment(HitsTotal)
	counters.Increment(stats_hits)
	counters.Add(TimeTotal, spent)
	counters.Add(stats_time, spent)

  trace := fmt.Sprintf(
    "%d - INF local peer %s %d %d %d",
    os.Getpid(), req.URL.Path,
    chunkreq.status, spent, chunkreq.bytes_out)
  logger, _ := syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_LOCAL0, 0)
  logger.Print(trace)
}
