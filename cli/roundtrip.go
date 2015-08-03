/*
OpenIO SDS Go client SDK
Copyright (C) 2015 OpenIO

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3.0 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library.
*/

package main

import (
	"bytes"
	"github.com/jfsmig/oio-go/sdk"
	"io"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {

	rand.Seed(time.Now().UnixNano())

	var ok bool
	var err error
	var ns string = "NS"
	var acct string = "ACCT"
	var ref string = "JFS" + strconv.FormatUint(uint64(rand.Int63()), 10)
	var dir oio.Directory
	var bkt oio.Container
	var obj oio.ObjectStorage

	cfg := oio.MakeStaticConfig()
	cfg.Set("NS", "proxy", "127.0.0.1:6002")
	cfg.Set("NS", "autocreate", "true")

	dir, _ = oio.MakeDirectoryClient(ns, cfg)
	bkt, _ = oio.MakeContainerClient(ns, cfg)
	obj, _ = oio.MakeObjectStorageClient(dir, bkt)

	log.Println("+++ References")
	for i := 0; i < 2; i++ {
		ok, err = dir.HasReference(acct, ref)
		if err != nil {
			log.Fatal("HasReference() error: ", err)
		}

		if ok {
			log.Println("Reference present")
		} else {
			ok, err = dir.CreateReference(acct, ref)
			if err != nil {
				log.Fatal("CreateReference() error: ", err)
			}
			if ok {
				log.Println("Reference created")
			} else {
				log.Println("Reference already present")
			}
		}

		ok, err = dir.DeleteReference(acct, ref)
		if err != nil {
			log.Fatal("DeleteReference() error: ", err)
		}
	}

	log.Println("+++ Container")
	for i := 0; i < 2; i++ {
		ok, err = bkt.HasContainer(acct, ref)
		if err != nil {
			log.Fatal("HasContainer() error: ", err)
		}
		if !ok {
			ok, err = bkt.CreateContainer(acct, ref)
			if err != nil {
				log.Fatal("CreateContainer() error: ", err)
			}
		}
		ok, err = bkt.DeleteContainer(acct, ref)
		if err != nil {
			log.Fatal("DeleteContainer() error: ", err)
		}
	}

	log.Println("+++ Contents")
	for i := 0; i < 2; i++ {
		var size uint64 = 4000
		bulk := make([]byte, size)
		bulkReader := bytes.NewReader(bulk)
		err = obj.PutContent(acct, ref, "plop", size, bulkReader)
		log.Println("PutContent(): ", err)
	}
	for i := 0; i < 2; i++ {
		var dl io.ReadCloser
		dl, err = obj.GetContent(acct, ref, "plop")
		if err != nil {
			log.Fatal("GetContent() error: ", err)
		} else {
			log.Println("GetContent() downloading ...")
			var buf []byte = make([]byte, 8192)
			for {
				if n, err := dl.Read(buf); err != nil {
					if err == io.EOF {
						log.Println("... EOF!")
						break
					} else {
						log.Fatal("GetContent() consumer error: ", err)
					}
				} else {
					log.Println("... consumed ", n, " bytes")
				}
			}
			dl.Close()
		}
	}
	for i := 0; i < 2; i++ {
		err = obj.DeleteContent(acct, ref, "plop")
		log.Println("DeleteContent(): ", err)
	}

}
