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
	"strconv"
	"strings"
)

// Dummy Config implementation where everything is stored in a single map.
// The map keys are encoded as "ns.key". This means the dot '.' is forbidden
// in the namespace names to work properly.
type StaticConfig struct {
	pairs map[string]string
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

func getProxyUrl(ns string, cfg Config) string {
	u, err := cfg.GetString(ns, KeyProxy)
	if err != nil {
		return "PROXY-NOT-CONFIGURED"
	}
	return u
}

func MakeStaticConfig() *StaticConfig {
	cfg := new(StaticConfig)
	cfg.pairs = make(map[string]string)
	return cfg
}
