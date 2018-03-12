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

import (
    "errors"
    "log"
    "os"
    "syscall"
    "path/filepath"
	// "bytes"
	// "compress/zlib"
	// "crypto/md5"
	// "encoding/hex"
	// "fmt"
	// "io"
	// "io/ioutil"
	// "log"
	// "net/http"
	//
	// "strings"
	// "strconv"
)

var (
    ErrInvalidMove = errors.New("Invalid move operation")
)

// MoveOrder -- contains information about a single chunk to move
// NOTE: We are supposing that filerepo list is immutable during runtime?
type MoveOrder struct {
    src int
    chunkid string
    op int  // 0: down, 1: del, 2 up
}

// Mover -- a single chunkRepo entity that moves objects
type Mover struct {
	queue chan MoveOrder
    cr chunkRepository
    len int
}

// MakeMover -- creates a mover entity
func MakeMover(cr chunkRepository) *Mover  {
	m := new(Mover)
	m.cr = cr
    m.queue = make(chan MoveOrder)
    m.len = len(m.cr.subs) - 2
	return m
}

// Run -- run the mover
func (m *Mover) Run() {
    for {
        order := <- m.queue
        if order.src < 0 ||
           order.src > m.len + 1 ||
           (order.src < 1 && order.op == 2) ||
           (order.src > m.len && order.op == 0) {
            log.Println(ErrInvalidMove)
            continue
        }
        switch order.op {
        case 0, 2:
            if err := m.move(m.cr.subs[order.src], m.cr.subs[order.src + (1 - order.op)], order.chunkid); err != nil {
                log.Println(err)
            }
            log.Printf("Move order completed %s", order.chunkid)
            break
        }
    }
}

func (m* Mover) movePut(dst Repository, name string, p string, cl int64, alloc bool) (*os.File, error) {
    tmpPath := p + ".pending"
	f, err := os.OpenFile(tmpPath, putOpenFlags, putOpenMode)
	if err == nil {
		if alloc {
			if err := syscall.Fallocate(int(f.Fd()), 0, 0, cl); err != nil {
				f.Close()
				os.Remove(tmpPath)
				return nil, err
			}
		}
		if _, err = os.Stat(p); err == nil {
			f.Close()
			os.Remove(tmpPath)
			return nil, ErrChunkExists
		}
        return f, nil

	} else if os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(p), putMkdirMode)
        dstDir, err := os.OpenFile(filepath.Dir(p), os.O_RDONLY, 0)
        defer dstDir.Close()
        if err := syscall.Fsync(int(dstDir.Fd())); err != nil {
            return nil, err
        }
		if err == nil {
			return m.movePut(dst, name, p, cl, alloc)
		}
	}
	return nil, err
}

func (m *Mover) move(src Repository, dst Repository, chunkid string) error {
    if names, err := m.cr.NameToPath(chunkid); err != nil {
		return err
	} else {
		for _, name := range names {
			if v, _ := src.Has(name); v {
                srcPath, err := src.NameToPath(name);
                if err != nil {
            		return err
            	}
                srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
                if err != nil {
                    return err
                }

                // Get src file length
                fi, err := srcFile.Stat()
                if err != nil {
                    return err
                }
                cl := fi.Size()
                srcFD := int(srcFile.Fd())

                dstPath, err := dst.NameToPath(name)
            	if err != nil {
            		return err
            	}

                dstFile, err := m.movePut(dst, name, dstPath, cl, true)
                if err != nil {
                    return err
                }
                dstFD := int(dstFile.Fd())

                if _, err := syscall.Sendfile(dstFD, srcFD, nil, int(cl)); err != nil {
                    return err
                }
                // Commit

                if err := syscall.Fsync(dstFD); err != nil {
                    return err
                }

                srcFile.Close()
                dstFile.Close()

                // Cleanup moved file
                if _, err := os.Stat(srcPath); err != nil {
                    if _, err := os.Stat(dstPath + ".pending"); err == nil {
                        if err := os.Remove(dstPath + ".pending"); err != nil {
                            return err
                        }
                    }
                    return nil
                }

                if err := syscall.Rename(dstPath + ".pending", dstPath); err != nil {
                    return err
                }
                if err := os.Symlink(dstPath, srcPath + ".lnk"); err != nil {
                    return err
                }
                if err := os.Rename(srcPath + ".lnk", srcPath); err != nil {
                    return err
                }
			} else {
                // TODO: handle this
            }
		}
		return nil
	}
}
