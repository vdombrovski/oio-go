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
Wraps the sile repository to add chunk-related handlings, e.g. transparent compression,
alternative file names, etc.
*/

import (
	"os"
	"strings"
)

const (
	compressMethodGZIP = "gzip"
	compressMethodLZ   = "lz"
	compressMethodBZ2  = "bz2"
)
const (
	compressEnabledDefault = false
	compressMethodDefault  = compressMethodGZIP
)

type chunkRepository struct {
	sub             Repository
	compressEnabled bool
	compressMethod  string
	accepted        [32]byte
}

func MakeChunkRepository(sub Repository) *chunkRepository {
	if sub == nil {
		panic("BUG : bad repository initiation")
	}
	r := new(chunkRepository)
	r.sub = sub
	r.compressEnabled = compressEnabledDefault
	r.compressMethod = compressMethodDefault

	hexa := []byte{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'A', 'B', 'C', 'D', 'E', 'F'}
	for _, c := range hexa {
		r.accepted[c/8] |= (1 << (c % 8))
	}
	return r
}

func (self *chunkRepository) Lock(ns, url string) error {
	return self.sub.Lock(ns, url)
}

func (self *chunkRepository) Has(name string) (bool, error) {
	if names, err := self.nameToPath(name); err != nil {
		return false, err
	} else {
		for _, name := range names {
			if v, _ := self.sub.Has(name); v {
				return true, nil
			}
		}
		return false, nil
	}
}

func (self *chunkRepository) Del(name string) error {
	if names, err := self.nameToPath(name); err != nil {
		return err
	} else {
		for _, name := range names {
			err = self.sub.Del(name)
			if err == nil {
				return nil // Success!
			} else if err != os.ErrNotExist && !os.IsNotExist(err) {
				return err
			}
		}
		return os.ErrNotExist
	}
}

func (self *chunkRepository) Get(name string) (FileReader, error) {
	if names, err := self.nameToPath(name); err != nil {
		return nil, err
	} else {
		for _, name := range names {
			r, err := self.sub.Get(name)
			if err == nil {
				return r, nil
			} else if err != os.ErrNotExist {
				return nil, err
			}
		}
		return nil, os.ErrNotExist
	}
}

func (self *chunkRepository) Put(name string) (FileWriter, error) {
	if names, err := self.nameToPath(name); err != nil {
		return nil, err
	} else {
		for _, name := range names {
			w, err := self.sub.Put(name)
			if err == nil {
				return w, nil
			} else if err != os.ErrNotExist {
				return nil, err
			}
		}
		return nil, os.ErrNotExist
	}
}

// Only accepts hexadecimal strings of 64 characters, and return potential
// matches
func (self *chunkRepository) nameToPath(name string) ([]string, error) {
	name = strings.ToUpper(name)
	if err := self.isValid(name); err != nil {
		return nil, err
	} else {
		tab := make([]string, 1)
		tab[0] = name
		return tab, nil
	}
}

func (self *chunkRepository) isValid(name string) error {
	var i int
	var n rune
	for i, n = range name {
		if !self.isValidChar(byte(n)) {
			return ErrInvalidChunkName
		}
	}
	if i == 63 {
		return nil
	}
	return ErrInvalidChunkName
}

func (self *chunkRepository) isValidChar(b byte) bool {
	return 0 != (self.accepted[b/8] & (1 << (b % 8)))
}
