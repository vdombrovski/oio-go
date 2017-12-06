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
	"bytes"
	"errors"
	"os"
	"log"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	hashStart    = true
	hashWidth    = 3
	hashDepth    = 1
	putOpenFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	putOpenMode  = 0644
	putMkdirMode = 0755
)

type NoopNotifier struct{}

func (self *NoopNotifier) NotifyPut(n string) {}
func (self *NoopNotifier) NotifyDel(n string) {}

type Notifier interface {
	NotifyPut(name string)
	NotifyDel(name string)
}

type FileRepository struct {
	notifier     Notifier
	root         string
	putOpenMode  os.FileMode
	putMkdirMode os.FileMode
	HashWidth    int
	HashDepth    int
	putOpenFlags int
	HashStart    bool
  sync_file    bool
  sync_dir     bool
}

func MakeFileRepository(root string, notifier Notifier) *FileRepository {
	r := new(FileRepository)
	if notifier == nil {
		r.notifier = &NoopNotifier{}
	} else {
		r.notifier = notifier
	}
	r.root = root
	r.HashStart = hashStart
	r.HashWidth = hashWidth
	r.HashDepth = hashDepth
	r.putOpenFlags = putOpenFlags
	r.putOpenMode = putOpenMode
	r.putMkdirMode = putMkdirMode
  r.sync_file = false
  r.sync_dir = true

	return r
}

func setOrHasXattr(path, n, v string) error {
	if err := syscall.Setxattr(path, n, []byte(v), 1); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return err
	}
	tab := make([]byte, 256)
	sz, err := syscall.Getxattr(path, n, tab)
	if err != nil {
		return err
	}
	if bytes.Equal([]byte(v), tab[:sz]) {
		return nil
	} else {
		return errors.New("XATTR mismatch")
	}
}

func (r *FileRepository) Lock(ns, url string) error {
	var err error
	err = setOrHasXattr(r.root, "user.rawx_server.address", url)
	if err != nil {
		return err
	}
	err = setOrHasXattr(r.root, "user.rawx_server.namespace", ns)
	if err != nil {
		return err
	}
	return nil
}

func (r *FileRepository) Has(name string) (bool, error) {
	if p, err := r.nameToPath(name); err != nil {
		return false, err
	} else if _, err := os.Stat(p); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (r *FileRepository) Del(name string) error {
	fullname := filepath.Join(r.root, name)
	err := os.Remove(fullname)
	if err == nil {
		r.notifier.NotifyDel(name)
	}
	return err
}

func realGet(p string) (FileReader, error) {
	if f, err := os.OpenFile(p, os.O_RDONLY, 0); err != nil {
		return nil, err
	} else {
		r := new(RealFileReader)
		r.impl = f
		r.path = p
		return r, nil
	}
}

func (r *FileRepository) Get(name string) (FileReader, error) {
	if p, err := r.nameToPath(name); err != nil {
		return nil, err
	} else {
		return realGet(p)
	}
}

func (r *FileRepository) realPut(n, p string) (FileWriter, error) {
  path_temp := p + ".pending"
	f, err := os.OpenFile(path_temp, r.putOpenFlags, r.putOpenMode)
	if err == nil {
    // Tempfile now open, ready to work with
    if _, err = os.Stat(p); err == nil {
      os.Remove(path_temp)
      f.Close()
      return nil, ErrChunkExists
    }

		return &RealFileWriter{
			name: p, path_final: p, path_temp: path_temp,
			impl: f, notifier: r.notifier,
      sync_file: r.sync_file, sync_dir: r.sync_dir,
		}, nil
	} else if os.IsNotExist(err) { // Lazy dir creation
		err = os.MkdirAll(filepath.Dir(p), r.putMkdirMode)
		if err == nil {
			return r.realPut(n, p)
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (r *FileRepository) Put(name string) (FileWriter, error) {
	if p, err := r.nameToPath(name); err != nil {
		return nil, err
	} else {
		return r.realPut(name, p)
	}
}

// Takes only the basename, check it is hexadecimal with a length of 64,
// and computes the hashed path
func (r *FileRepository) nameToPath(name string) (string, error) {

	// Sanity checks and cleanups
	if len(name) <= 0 {
		return "", os.ErrInvalid
	}
	name = strings.Replace(filepath.Clean(name), "/", "@", -1)

	// Hash computations
	tokens := make([]string, 0, 5)
	tokens = append(tokens, r.root)
	if r.HashStart { // Hash the beginning of the basename
		for i := 0; i < r.HashDepth; i++ {
			start := i * r.HashDepth
			tokens = append(tokens, name[start:start+r.HashWidth])
		}
	} else { // Hash the end of the basename
		for i := 0; i < r.HashDepth; i++ {
			start := len(name) - ((i + 1) * r.HashDepth)
			tokens = append(tokens, name[start:start+r.HashWidth])
		}
	}

	tokens = append(tokens, name)
	return filepath.Join(tokens...), nil
}

type RealFileWriter struct {
	name       string
	path_final string
	path_temp  string
	impl       *os.File
	notifier   Notifier
  sync_file  bool
  sync_dir   bool
}

func (w *RealFileWriter) Name() string {
	return w.name
}

func (w *RealFileWriter) Seek(o int64) error {
	_, err := w.impl.Seek(o, os.SEEK_SET)
	return err
}

func (w *RealFileWriter) SetAttr(n string, v []byte) error {
	err := syscall.Setxattr(w.path_temp, n, v, 0)
	return err
}

func (w *RealFileWriter) Sync() error {
	return w.impl.Sync()
}

func (w *RealFileWriter) Write(buf []byte) (int, error) {
	return w.impl.Write(buf)
}

func (w *RealFileWriter) Abort() error {
	os.Remove(w.path_temp)
	return w.impl.Close()
}

func (w *RealFileWriter) syncFile() {
  if w.sync_file {
    //w.impl.Sync()
    syscall.Fdatasync(int(w.impl.Fd()))
  }
}

func (w *RealFileWriter) syncDir() {
  if w.sync_dir {
    dir := filepath.Dir(w.path_final)
    if f, err := os.OpenFile(dir, os.O_RDONLY, 0); err == nil {
      f.Sync()
      f.Close()
    } else {
      log.Println("Directory sync error: ", err)
    }
  }
}

func (w *RealFileWriter) Commit() error {
  w.syncFile()
	err := w.impl.Close()
	if err == nil {
		err = os.Rename(w.path_temp, w.path_final)
		if err == nil {
      w.syncDir()
			w.notifier.NotifyPut(w.name)
		} else {
      log.Println("Rename error: ", err)
    }
	} else {
    log.Println("Close error: ", err)
  }
	if err != nil {
		os.Remove(w.path_temp)
	}
	return err
}

type RealFileReader struct {
	path string
	impl *os.File
}

func (r *RealFileReader) Name() string {
	return filepath.Base(r.path)
}

func (r *RealFileReader) Size() int64 {
	fi, _ := r.impl.Stat()
	return fi.Size()
}

func (r *RealFileReader) Seek(o int64) error {
	_, err := r.impl.Seek(o, os.SEEK_SET)
	return err
}

func (r *RealFileReader) Close() error {
	return r.impl.Close()
}

func (r *RealFileReader) Read(buf []byte) (int, error) {
	return r.impl.Read(buf)
}

func (r *RealFileReader) GetAttr(n string) ([]byte, error) {
	tmp := make([]byte, 256)
	sz, err := syscall.Getxattr(r.path, n, tmp)
	if err != nil {
		return nil, err
	} else {
		return tmp[:sz], nil
	}
}
