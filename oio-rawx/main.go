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

/*
Parses and checks the CLI arguments, then ties together a repository and a
http handler.
*/

import (
	"flag"
	"log"
	"log/syslog"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"os"
	"fmt"
)

func checkURL(url string) {
	addr, err := net.ResolveTCPAddr("tcp", url)
	if (err != nil || addr.Port <= 0) {
		log.Fatalf("%s is not a valid URL", url)
	}
}

func checkNS(ns string) {
	if ok, _ := regexp.MatchString("[0-9a-zA-Z]+(\\.[0-9a-zA-Z]+)*", ns); !ok {
		log.Fatalf("%s is not a valid namespace name", ns)
	}
}


func main() {
	nsPtr := flag.String("ns", "OPENIO", "Namespace to run on")
	addrPtr := flag.String("addr", "127.0.0.1:5999", "IP:PORT to run the rawx service on")

	flag.Usage = func() {
	        fmt.Fprintf(os.Stderr, "Usage of %s: [filerepo] (filerepo)\n", os.Args[0])
	        flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatalln("Missing target filerepo(s)")
	}

	checkNS(*nsPtr)
	checkURL(*addrPtr)

 	var filerepos = make([]*FileRepository, 0)

	for _, repo := range flag.Args() {
		basedir := filepath.Clean(repo)
		if !filepath.IsAbs(basedir) {
			log.Fatalf("Filerepo path must be absolute, got %s", basedir)
		}
		filerepos = append(filerepos, MakeFileRepository(basedir, nil))
	}

	if filerepos[0] == nil {
		log.Fatalln("Invalid filerepo")
	}

	// TODO: Make chunk repo out of multiple filerepos
	chunkrepo := MakeChunkRepository(filerepos[0])
	if err := chunkrepo.Lock(*nsPtr, *addrPtr); err != nil {
		log.Fatalf("Basedir cannot be locked with xattr : %s", err.Error())
	}

	prepareServe(*nsPtr, *addrPtr, chunkrepo)
}

func prepareServe(ns string, url string, chunkrepo *chunkRepository) {
	var rawxID string

	logAccess, _ := syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_LOCAL0, 0)
	logErr, _ := syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_LOCAL1, 0)

	rawx := rawxService{
		ns:            ns,
		id:            rawxID,
		url:           url,
		repo:          chunkrepo,
		compress:      false,
		logger_access: logAccess,
		logger_error:  logErr,
	}

	http.Handle("/chunk", &chunkHandler{&rawx})
	http.Handle("/info", &statHandler{&rawx})
	http.Handle("/stat", &statHandler{&rawx})

	// Some usages of the RAWX API don't use any prefix when calling
	// operations on chunks.
	http.Handle("/", &chunkHandler{&rawx})

	if err := http.ListenAndServe(rawx.url, nil); err != nil {
		log.Fatal("HTTP error : ", err)
	}
}
