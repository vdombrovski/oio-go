/*
OpenIO SDS Go client SDK
Copyright (C) 2017 OpenIO

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
	oio "github.com/jfsmig/oio-go/sdk"
	"log"
	"fmt"
)

func main() {
	cfg := oio.MakeStaticConfig()
	if err := cfg.LoadWithSystem(); err != nil {
		log.Println("Failed to load the system configuration: ", err)
	}
	if err := cfg.LoadWithLocal(); err != nil {
		log.Println("Failed to load the local configuration: ", err)
	}

	for _, ns := range cfg.Namespaces() {
		for _, k := range cfg.Keys(ns) {
			v, _ := cfg.GetString(ns, k)
			fmt.Println(ns, k, v)
		}
	}
}
