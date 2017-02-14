// OpenIO SDS Go client SDK
// Copyright (C) 2017 OpenIO
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
	"testing"
)

func TestConfig_Init(t *testing.T) {
	cfg := MakeStaticConfig()
	if cfg == nil {
		t.Fatal("StaticConfig allocation failed")
	}
}

func TestConfig_LoadValidContent(t *testing.T) {
	var str, value string
	var err error
	var cfg *StaticConfig

	if cfg = MakeStaticConfig(); cfg == nil {
		t.Fatal("StaticConfig allocation failed")
	}
	if err = cfg.LoadWithContent([]byte("")); err != nil {
		t.Fatal("LoadWithContent failed: ", err)
	}
	if err = cfg.LoadWithContent([]byte(" ")); err != nil {
		t.Fatal("LoadWithContent failed: ", err)
	}
	if err = cfg.LoadWithContent([]byte("#")); err != nil {
		t.Fatal("LoadWithContent failed: ", err)
	}
	if err = cfg.LoadWithContent([]byte("[plop]")); err != nil {
		t.Fatal("LoadWithContent failed: ", err)
	}
	value = "127.0.0.1:6000"
	if err = cfg.LoadWithContent([]byte("[plop]\nzookeeper="+value)); err != nil {
		t.Fatal("LoadWithContent failed: ", err)
	}
	if str, err = cfg.GetString("plop", "zookeeper"); err != nil {
		t.Fatal("LoadWithContent: expected key not found")
	} else if str != value {
		t.Fatal("LoadWithContent: expected value not found")
	}
}

func TestConfig_LoadInValidContent(t *testing.T) {
	cfg := MakeStaticConfig()
	if cfg == nil {
		t.Fatal("StaticConfig allocation failed")
	}
	err := cfg.LoadWithContent([]byte("?"))
	if err == nil {
		t.Fatal("LoadWithContent succeeded: ", err)
	}
}
