// OpenIO SDS Go rawx
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
// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	hashStart    = true
	hashWidth    = 2
	hashDepth    = 2
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
	HashStart    bool
	HashWidth    int
	HashDepth    int
	putOpenFlags int
	putOpenMode  os.FileMode
	putMkdirMode os.FileMode
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

	return r
}


func setOrHasXattr (path,n,v string) error {
	if err := syscall.Setxattr(path, n, []byte(v), 1); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return err
	}
	tab := make([]byte,256)
	sz,err := syscall.Getxattr(path, n, tab)
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
	f, err := os.OpenFile(p, r.putOpenFlags, r.putOpenMode)
	if err == nil {
		w := new(RealFileWriter)
		w.notifier = r.notifier
		w.impl = f
		w.path = p
		return w, nil
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
	bn := ""
	if r.HashStart { // Hash the beginning of the basename
		for i := 0; i < r.HashDepth; i++ {
			if len(bn) > 0 {
				bn += "/"
			}
			start := i * r.HashDepth
			bn += name[start : start+r.HashWidth]
		}
	} else { // Hash the end of the basename
		for i := 0; i < r.HashDepth; i++ {
			if len(bn) > 0 {
				bn += "/"
			}
			start := len(name) - ((i + 1) * r.HashDepth)
			bn += name[start : start+r.HashWidth]
		}
	}
	return filepath.Join(r.root, bn), nil
}

type RealFileWriter struct {
	notifier Notifier
	path     string
	name     string
	impl     *os.File
}

func (w *RealFileWriter) Name() string {
	return filepath.Base(w.path)
}

func (w *RealFileWriter) Seek(o int64) error {
	_, err := w.impl.Seek(o, os.SEEK_SET)
	return err
}

func (w *RealFileWriter) SetAttr(n string, v []byte) error {
	err := syscall.Setxattr(w.path, n, v, 0)
	return err
}

func (w *RealFileWriter) Sync() error {
	return w.impl.Sync()
}

func (w *RealFileWriter) Commit() error {
	w.impl.Sync()
	err := w.impl.Close()
	if err == nil {
		w.notifier.NotifyPut(w.name)
	}
	return err
}

func (w *RealFileWriter) Abort() error {
	os.Remove(w.path)
	return w.impl.Close()
}

func (w *RealFileWriter) Write(buf []byte) (int, error) {
	return w.impl.Write(buf)
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
