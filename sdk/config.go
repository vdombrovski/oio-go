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
	"github.com/go-ini/ini"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Dummy Config implementation where everything is stored in a single map.
// The map keys are encoded as "ns.key". This means the dot '.' is forbidden
// in the namespace names to work properly.
type StaticConfig struct {
	pairs map[string]string
}

// Returns the list of namespaces known in the (loaded) configuration
func (cfg *StaticConfig) Namespaces() []string {
	tmp := make(map[string]bool)
	for k,_ := range cfg.pairs {
		if idx := strings.Index(k, "/"); idx > 0 {
			tmp[k[:idx]] = true
		}
	}
	out := make([]string,0)
	for k,_ := range tmp {
		out = append(out, k)
	}
	return out
}

// Returns all the keys known for the given namespace in the (loaded)
// configuration
func (cfg *StaticConfig) Keys(ns string) []string {
	out := make([]string,0)
	for k,_ := range cfg.pairs {
		if idx := strings.Index(k, "/"); idx > 0 {
			if k[:idx] == ns {
				out = append(out,k[idx+1:])
			}
		}
	}
	return out
}

// Sets a configuration key for the given namespace
func (cfg *StaticConfig) Set(ns, key, value string) {
	k := strings.Join([]string{ns, key}, "/")
	cfg.pairs[k] = value
}

// Get the raw value of a configuration key, if set. Otherwise an error is
//returned.
func (cfg *StaticConfig) GetString(ns, key string) (string, error) {
	k := strings.Join([]string{ns, key}, "/")
	v, ok := cfg.pairs[k]
	if !ok {
		return "", ErrorNotFound
	}
	return v, nil
}

func (cfg *StaticConfig) GetBool(ns, key string) (bool, error) {
	s, err := cfg.GetString(ns, key)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(s)
}

func (cfg *StaticConfig) GetInt(ns, key string) (int64, error) {
	s, err := cfg.GetString(ns, key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// Build an empty StaticConfig
func MakeStaticConfig() *StaticConfig {
	cfg := new(StaticConfig)
	cfg.pairs = make(map[string]string)
	return cfg
}

func (cfg *StaticConfig) LoadWithContent(content []byte) error {
	kvf, err := ini.Load(content)
	if err != nil {
		return err
	}
	for _, section := range kvf.Sections() {
		for _, key := range section.Keys() {
			cfg.pairs[section.Name() + "/" + key.Name()] = key.Value()
		}
	}
	return nil
}

// Loads the given StaticConfig with the content of the given file
func (cfg *StaticConfig) LoadWithFile(path string) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return cfg.LoadWithContent(content)
}

// Loads the given StaticConfig with the system-wide configuration
func (cfg *StaticConfig) LoadWithSystem() error {
	var err error
	err = cfg.LoadWithFile("/etc/oio/sds.conf")
	if err != nil {
		return err
	}

	files, _ := filepath.Glob("/etc/oio/sds.conf.d/*.conf")
	for _, file := range files {
		if err = cfg.LoadWithFile(file); err != nil {
			log.Println("StaticConfig: Failed to load [%s]", file)
		}
	}

	return nil
}

// Loads the given StaticConfig with the configuration in the homedir of
// the user.
func (cfg *StaticConfig) LoadWithLocal() error {
	if home, ok := os.LookupEnv("HOME"); !ok {
		return errors.New("No HOME in ENV")
	} else {
		return cfg.LoadWithFile(home + "/.oio/sds.conf")
	}
}

func getProxyUrl(ns string, cfg Config) string {
	u, err := cfg.GetString(ns, KeyProxy)
	if err != nil {
		return "PROXY-NOT-CONFIGURED"
	}
	return u
}

func getProxyConscienceUrl(ns string, cfg Config) string {
	u, err := cfg.GetString(ns, KeyProxyConscience)
	if err != nil {
		return getProxyUrl(ns, cfg)
	}
	return u
}

func getProxyContainerUrl(ns string, cfg Config) string {
	u, err := cfg.GetString(ns, KeyProxyContainer)
	if err != nil {
		return getProxyUrl(ns, cfg)
	}
	return u
}

func getProxyDirectoryUrl(ns string, cfg Config) string {
	u, err := cfg.GetString(ns, KeyProxyDirectory)
	if err != nil {
		return getProxyUrl(ns, cfg)
	}
	return u
}
