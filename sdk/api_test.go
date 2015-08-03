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
	"bytes"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

var ns, acct, ref, srvtype, path string = "NS", "ACCT", "REF", "meta2", "path"

func TestContainer(t *testing.T) {
	var ok bool
	var err error
	var cli Container

	cfg := MakeStaticConfig()
	cfg.Set(ns, KeyAutocreate, "true")
	cfg.Set(ns, KeyProxy, "127.0.0.1:6002")
	cli, err = MakeContainerClient(ns, cfg)

	ok, err = cli.HasContainer(acct, ref)
	if err != nil {
		log.Fatal("HasContainer error: ", err)
	}
	if ok {
		log.Println("HasContainer: already present")
	}

	ok, err = cli.CreateContainer(acct, ref)
	if err != nil {
		log.Fatal("CreateContainer error: ", err)
	}

	dump, err := cli.ListContents(acct, ref)
	if err != nil {
		log.Fatal("ListContents error: ", err)
	}
	log.Println("List (post-create): ", dump)

	chunks, err := cli.GenerateContent(acct, ref, path, 1024)
	if err != nil {
		log.Fatal("GenerateContent error: ", err)
	}

	err = cli.PutContent(acct, ref, path, 1024, chunks)
	if err != nil {
		log.Fatal("PutContent error: ", err)
	}

	dump, err = cli.ListContents(acct, ref)
	if err != nil {
		log.Fatal("ListContents error: ", err)
	}
	log.Println("List (post-put): ", dump)

	ok, err = cli.DeleteContent(acct, ref, path)
	if err != nil {
		log.Fatal("DeleteContent error: ", err)
	}

	dump, err = cli.ListContents(acct, ref)
	if err != nil {
		log.Fatal("ListContents error: ", err)
	}
	log.Println("List (post-delete): ", dump)

	ok, err = cli.DeleteContainer(acct, ref)
	if err != nil {
		log.Fatal("DeleteContainer error: ", err)
	}
}

func TestDirectory(t *testing.T) {
	var err error
	var ok bool
	var tab []Service
	var dir Directory

	cfg := MakeStaticConfig()
	cfg.Set(ns, KeyAutocreate, "true")
	cfg.Set(ns, KeyProxy, "127.0.0.1:6002")

	dir, err = MakeDirectoryClient(ns, cfg)
	if err != nil {
		panic(err)
	}

	ok, _ = dir.HasReference(acct, ref)

	if !ok {
		ok, err = dir.CreateReference(acct, ref)
		if err != nil {
			log.Fatal("CreateReference error: ", err)
		}
	}

	props := map[string]string{"user.x": "value", "user.y": "value"}
	ok, err = dir.SetProperties(acct, ref, props)
	if err != nil {
		log.Fatal("SetProperties error: ", err)
	}
	log.Println("SetProperties: ", props)

	props, err = dir.GetProperties(acct, ref)
	if err != nil {
		log.Fatal("GetProperties error: ", err)
	}
	log.Println("GetProperties: ", props)

	keys := []string{"user.y"}
	ok, err = dir.DeleteProperties(acct, ref, keys)
	if err != nil {
		log.Fatal("DeleteProperties error: ", err)
	}

	props, err = dir.GetProperties(acct, ref)
	if err != nil {
		log.Fatal("GetProperties error: ", err)
	}
	log.Println("Properties: ", props)

	tab, err = dir.LinkServices(acct, ref, srvtype)
	if err != nil {
		log.Fatal("LinkServices error: ", err)
	}
	log.Println("Link: ", tab)

	tab, err = dir.ListServices(acct, ref, srvtype)
	if err != nil {
		log.Fatal("DumpReference error: ", err)
	}
	log.Println("List: ", tab)

	tab, err = dir.RenewServices(acct, ref, srvtype)
	if err != nil {
		log.Fatal("RenewServices error: ", err)
	}
	log.Println("Renewed: ", tab)

	dump, err := dir.DumpReference(acct, ref)
	if err != nil {
		log.Fatal("DumpReference error: ", err)
	}
	log.Println("Reference services: ", dump)

	ok, err = dir.UnlinkServices(acct, ref, srvtype)
	if err != nil {
		log.Fatal("UnlinkServices error: ", err)
	}

	keys = []string{}
	ok, err = dir.DeleteProperties(acct, ref, keys)
	if err != nil {
		log.Fatal("DeleteProperties error: ", err)
	}

	ok, err = dir.DeleteReference(acct, ref)
	if err != nil {
		log.Fatal("DeleteReference error: ", err)
	}
}

func TestObjectStorage(t *testing.T) {
	var size uint64 = 4000
	var err error

	cfg := MakeStaticConfig()
	cfg.Set(ns, KeyAutocreate, "true")
	cfg.Set(ns, KeyProxy, "127.0.0.1:6002")

	bulk := make([]byte, size)
	bulkReader := bytes.NewReader(bulk)

	dir, err := MakeDirectoryClient(ns, cfg)
	container, err := MakeContainerClient(ns, cfg)
	obj, err := MakeObjectStorageClient(dir, container)

	log.Println("Ready...")
	err = obj.PutContent(acct, ref, path, size, bulkReader)
	if err != nil {
		log.Fatal("PutContent error: ", err)
	}

	dl, err := obj.GetContent(acct, ref, path)
	if err != nil {
		log.Fatal("GetContent error: ", err)
	}
	dl.Close()
}

func TestMain(m *testing.M) {
	ref = strconv.FormatInt(rand.Int63n(1024*1024), 16)
	os.Exit(m.Run())
}
