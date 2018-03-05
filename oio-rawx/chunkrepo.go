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
	"errors"
)

type chunkRepository struct {
	subs     []Repository
	sub      Repository // Temp (Compat)
	lrepo    Repository
	accepted [32]byte
	mover *Mover
}

func MakeChunkRepository(lrepo Repository, subs []Repository) *chunkRepository {
	// if sub == nil {
	// 	panic("BUG : bad repository initiation")
	// }
	r := new(chunkRepository)
	r.subs = subs
	r.sub = subs[0]
	r.lrepo = lrepo

	return r
}

func (cr *chunkRepository) PushMoveOrder(src int, chunkid string) {
	cr.mover.queue <- MoveOrder{
		src: src,
		chunkid: chunkid,
		op: 0,
	}
}

func (self *chunkRepository) Lock(ns, url string) error {
	if err := self.lrepo.Lock(ns, url); err != nil {
		return err
	}
	for _, sub := range self.subs {
		if err := sub.Lock(ns, url); err != nil {
			return err
		}
	}
	return nil
}

func (self *chunkRepository) Has(name string) (bool, error) {
	if names, err := self.NameToPath(name); err != nil {
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
	if names, err := self.NameToPath(name); err != nil {
		return err
	} else {
		for _, name := range names {
			for _, sub := range self.subs {
				err = sub.Del(name)
				if err == nil {
					return nil
				} else if !os.IsNotExist(err) {
					return err
				}
			}
		}
		return os.ErrNotExist
	}
}

func (self *chunkRepository) Get(name string) (FileReader, error) {
	names, err := self.NameToPath(name)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		for _, sub := range self.subs {
			r, err := sub.Get(name)
			if err == nil {
				return r, nil
			} else if !os.IsNotExist(err) {
				return nil, err
			}
		}
	}
	return nil, os.ErrNotExist
}


// Put -- lock chunk in lrepo then get the first allocatable filerepo
// TODO: Use struct here  (too many args)
func (self *chunkRepository) Put(name string, cl int64, alloc bool) (int, FileWriter, FileWriter, error) {
	names, err := self.NameToPath(name);
	if err != nil {
		return 0, nil, nil, err
	}
	_, lw, err := putOne(self.lrepo, names, 0, false)
	if err != nil {
		// TODO: handle this
	}
	for _, sub := range self.subs {
		src, w, err := putOne(sub, names, cl, true)
		if err == ErrChunkExists {
			return 0, lw, nil, err
		}
		if err == nil {
			return src, lw, w, nil
		}
	}
	return 0, lw, nil, errors.New("No more filerepos to try")
}

func putOne(sub Repository, names []string, cl int64, alloc bool) (int, FileWriter, error) {
	for src, name := range names {
		_, w, err := sub.Put(name, cl, alloc)
		if err == nil {
			return src, w, nil
		} else if err != os.ErrNotExist {
			return 0, nil, err
		}
	}
	return 0, nil, nil
}

// Only accepts hexadecimal strings of 64 characters, and return potential
// matches
func (self *chunkRepository) NameToPath(name string) ([]string, error) {
	name = strings.ToUpper(name)
	if !isValidString(name, 64) {
		return nil, ErrInvalidChunkName
	} else {
		tab := make([]string, 1)
		tab[0] = name
		return tab, nil
	}
}

func (self *chunkRepository) List(marker, prefix string, max int) (ListSlice, error) {
	out := ListSlice{make([]string, 0), false}

	if len(marker) > 0 && !isValidString(marker, 0) {
		return out, ErrListMarker
	}
	if len(prefix) > 0 && !isValidString(prefix, 0) {
		return out, ErrListPrefix
	}
	return self.sub.List(marker, prefix, max)
}
