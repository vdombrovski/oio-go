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
	"strings"
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

func checkMakeFileRepo(dir string) *FileRepository {
	basedir := filepath.Clean(dir)
	if !filepath.IsAbs(basedir) {
		log.Fatalf("Filerepo path must be absolute, got %s", basedir)
	}
	return MakeFileRepository(basedir, nil)
}


func main() {
	_ = flag.String("D", "UNUSED", "Unused compatibility flag")
	confPtr := flag.String("f", "/etc/oio/sds/OPENIO/rawx-0/rawx-0-httpd.conf", "Absolute path to config file")
	flag.Parse()

	opts, err := ReadConfig(*confPtr)
	if err != nil {
		log.Fatalf("Exiting with error %s", err.Error())
	}

	checkNS(opts["ns"])
	checkURL(opts["addr"])

 	var filerepos []Repository

	if _, ok := opts["filerepos"]; ok {
		for _, dir := range strings.Split(opts["filerepos"], ",") {
			filerepos = append(filerepos, checkMakeFileRepo(dir))
		}
	} else {
		// Fallback to old rawx
		filerepos = append(filerepos, checkMakeFileRepo(opts["filerepo"]))
	}

	chunkrepo := MakeChunkRepository(filerepos)
	if err := chunkrepo.Lock(opts["ns"], opts["addr"]); err != nil {
		log.Fatalf("Basedir cannot be locked with xattr : %s", err.Error())
	}

	mover := MakeMover(*chunkrepo)
	chunkrepo.mover = mover
	// TODO: Maybe implement as a pool
	go mover.Run()
	prepareServe(opts["ns"], opts["addr"], chunkrepo)
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
