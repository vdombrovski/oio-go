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

	var dir oio.Directory
	var bkt oio.Container
	var obj oio.ObjectStorage

	var ns string = "NS"

	name := oio.FlatName{
		N: ns,
		A: "ACCT",
		U: "JFS" + strconv.FormatUint(uint64(rand.Int63()), 10),
		P: "plop",
	}

	cfg := oio.MakeStaticConfig()
	cfg.Set(ns, "proxy", "127.0.0.1:6002")
	cfg.Set(ns, "autocreate", "true")

	dir, _ = oio.MakeDirectoryClient(ns, cfg)
	bkt, _ = oio.MakeContainerClient(ns, cfg)
	obj, _ = oio.MakeObjectStorageClient(dir, bkt)

	log.Println("+++ Users")
	for i := 0; i < 2; i++ {
		ok, err = dir.HasUser(&name)
		if err != nil {
			log.Fatal("HasUser() error: ", err)
		}

		if ok {
			log.Println("User present")
		} else {
			ok, err = dir.CreateUser(&name)
			if err != nil {
				log.Fatal("CreateUser() error: ", err)
			}
			if ok {
				log.Println("User created")
			} else {
				log.Println("User already present")
			}
		}

		ok, err = dir.DeleteUser(&name)
		if err != nil {
			log.Fatal("DeleteUser() error: ", err)
		}
	}

	log.Println("+++ Container")
	for i := 0; i < 2; i++ {
		ok, err = bkt.HasContainer(&name)
		if err != nil {
			log.Fatal("HasContainer() error: ", err)
		}
		if !ok {
			ok, err = bkt.CreateContainer(&name)
			if err != nil {
				log.Fatal("CreateContainer() error: ", err)
			}
		}
		ok, err = bkt.DeleteContainer(&name)
		if err != nil {
			log.Fatal("DeleteContainer() error: ", err)
		}
	}

	log.Println("+++ Contents")
	for i := 0; i < 2; i++ {
		var size uint64 = 4000
		bulk := make([]byte, size)
		bulkReader := bytes.NewReader(bulk)
		err = obj.PutContent(&name, size, bulkReader)
		log.Println("PutContent(): ", err)
	}
	for i := 0; i < 2; i++ {
		var dl io.ReadCloser
		dl, err = obj.GetContent(&name)
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
		err = obj.DeleteContent(&name)
		log.Println("DeleteContent(): ", err)
	}

}
