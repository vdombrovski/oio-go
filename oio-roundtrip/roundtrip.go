/*
OpenIO SDS Go client SDK
Copyright (C) 2015-2017 OpenIO

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
	"flag"
	oio "github.com/jfsmig/oio-go/sdk"
	"io"
	"log"
	"os"
)

func main() {

	var ns, acct, user, subtype, path string
	var ok bool
	var err error
	var dir oio.Directory
	var bkt oio.Container
	var obj oio.ObjectStorage

	flag.StringVar(&ns, "ns", os.Getenv("OIO_NS"), "Namespace (mandatory)")
	flag.StringVar(&acct, "account", os.Getenv("OIO_ACCOUNT"), "Account (optional)")
	flag.StringVar(&user, "user", os.Getenv("OIO_USER"), "User (optional)")
	flag.StringVar(&path, "path", os.Getenv("OIO_PATH"), "Path (optional)")
	flag.StringVar(&subtype, "type", os.Getenv("OIO_TYPE"), "Service subtype (optional)")
	flag.Parse()

	if ns == "" {
		log.Fatal("Namespace is not set")
	}
	if acct == "" {
		log.Fatal("Account is not set")
	}

	name := oio.FlatName{N: ns, A: acct, U: user, P: path}

	cfg := oio.MakeStaticConfig()
	if err := cfg.LoadWithLocal(); err != nil {
		log.Fatal("Failed to load all the local configuration")
	}
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
		} else {
			log.Println("User deleted")
		}
	}

	log.Println("+++ Container")
	for i := 0; i < 2; i++ {
		ok, err = bkt.HasContainer(&name)
		if err != nil {
			log.Fatal("HasContainer() error: ", err)
		}
		if !ok {
			ok, err = bkt.CreateContainer(&name, true)
			if err != nil {
				log.Fatal("CreateContainer() error: ", err)
			} else if ok {
				log.Println("Container created")
			} else {
				log.Println("Container nor created (already present)")
			}
		} else {
			log.Println("Container already present")
		}
		ok, err = bkt.DeleteContainer(&name)
		if err != nil {
			log.Fatal("DeleteContainer() error: ", err)
		} else {
			log.Println("Container deleted")
		}
	}

	log.Println("+++ Contents")
	for i := 0; i < 2; i++ {
		var size uint64 = 4000
		bulk := make([]byte, size)
		bulkReader := bytes.NewReader(bulk)
		err = obj.PutContent(&name, size, true, bulkReader)
		if err != nil {
			log.Fatal("PutContent(): ", err)
		} else {
			log.Println("Content uploaded")
		}
	}
	for i := 0; i < 2; i++ {
		var dl io.ReadCloser
		dl, err = obj.GetContent(&name)
		if err != nil {
			log.Fatal("GetContent() error: ", err)
		} else {
			var total uint64 = 0
			var buf []byte = make([]byte, 8192)
			for {
				if n, err := dl.Read(buf); err == nil {
					total = total + uint64(n)
				} else if err == io.EOF {
					break
				} else {
					log.Fatal("GetContent() consumer error: ", err)
				}
			}
			log.Println("Content downloaded (", total, " bytes)")
			dl.Close()
		}
	}

	err = obj.DeleteContent(&name)
	if err != nil {
		log.Fatal("DeleteContent(): ", err)
	} else {
		log.Println("Content deleted")
	}
}
