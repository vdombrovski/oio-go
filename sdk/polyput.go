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
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

type keyValue struct {
	key   string
	value string
}

type polyPut struct {
	headers []keyValue
	urls    []string
}

type subReq struct {
	err   error
	done  bool
	req   *http.Request
	wg    *sync.WaitGroup
	input chan []byte
	ready chan interface{}
	rest  []byte
}

type polyPutSource interface {
	Len() int64
	Read([]byte) (int, error)
	Close() error
}

func makePolyPut() polyPut {
	var pp polyPut
	pp.headers = make([]keyValue, 0)
	pp.urls = make([]string, 0)
	return pp
}

func (pp *polyPut) addHeader(key, value string) {
	pp.headers = append(pp.headers, keyValue{key: key, value: value})
}

func (pp *polyPut) addTarget(url string) {
	pp.urls = append(pp.urls, url)
}

func (pp *polyPut) do(src polyPutSource) error {
	if src == nil {
		panic("Invalid input")
	}

	var err error
	var wg sync.WaitGroup
	var subs []*subReq = make([]*subReq, 0)

	// create sub requests
	for _, url := range pp.urls {
		sub := &subReq{
			req:   nil,
			err:   nil,
			wg:    &wg,
			input: make(chan []byte, 8),
			ready: make(chan interface{}, 8),
			rest:  make([]byte, 0),
		}
		sub.req, err = http.NewRequest("PUT", url, sub)
		sub.req.Header.Set("Content-Type", "octet/stream")
		// No chunk encoding in this case, the size is known
		sub.req.ContentLength = src.Len()
		sub.req.TransferEncoding = make([]string, 0)
		for _, kv := range pp.headers {
			sub.req.Header.Set(kv.key, kv.value)
		}

		sub.req.Header.Set(RAWX_HEADER_PREFIX+"chunk-id", filepath.Base(url))
		subs = append(subs, sub)
	}

	// start the sub requests
	for _, sub := range subs {
		wg.Add(1)
		go sub.do()
	}

	// Now feed the sub requests
	for {
		var count int
		buf := make([]byte, 8192)
		count, err = src.Read(buf)
		if err != nil {
			for _, sub := range subs {
				close(sub.input)
			}
			break
		} else {
			for _, sub := range subs {
				if sub.err == nil && !sub.done {
					// send a new buffer when the subrequest tells it is ready
					// to accept it.
					if _, ok := <-sub.ready; ok {
						sub.input <- buf[:count]
					}
				}
			}
		}
	}

	// Wait for each subRequest to finish
	wg.Wait()

	// count the number of errors, we must reach the quorum
	var count_errors int
	for _, sub := range subs {
		if sub.err != nil {
			count_errors = count_errors + 1
		}
	}
	if count_errors >= 1+len(subs)/2 {
		if err != nil {
			err = errors.New("Quorum not reached")
		}
	}

	if err == io.EOF {
		return nil
	} else {
		return err
	}
}

func (sr *subReq) do() {
	defer sr.wg.Done()

	dial := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 1*time.Second)
	}
	transport := http.Transport{Dial: dial}
	client := http.Client{Transport: &transport}

	if resp, err := client.Do(sr.req); err != nil {
		sr.err = err
	} else {
		resp.Body.Close()
	}
}

// With Read(), the subReq class implements the io.Reader() interface.
// The http.HttpRequests requires a Reader() as a source for its input.
func (sr *subReq) Read(dst []byte) (int, error) {

	// factorizes the data consumption from src to dst
	doIt := func(src []byte) (int, error) {
		n := copy(dst, src)
		if n < len(src) {
			sr.rest = src[n:]
		} else {
			sr.rest = make([]byte, 0)
		}
		return n, nil
	}

	if sr.err != nil {
		return 0, sr.err
	}

	if len(sr.rest) > 0 {
		return doIt(sr.rest)
	}

	if sr.done {
		panic("input stream already closed")
	}

	sr.ready <- true
	if buf, ok := <-sr.input; !ok {
		close(sr.ready)
		sr.done = true
		return 0, io.EOF
	} else {
		return doIt(buf)
	}
}
