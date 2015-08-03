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

/*
OpenIO SDS (Software Defined Storage) client SDK.

  import (
  	"oio"
  	"os"
  )
  func main () {
  	ns := "MyNamespace"
  	cfg := oio.MakeStaticConfig()
  	cfg.Set(ns, "proxy", "127.0.0.1:6002")
  	obj, _ := oio.MakeDefaultObjectStorageClient(ns, cfg)
  	src, _ := os.Open("/Path/to/a/file")
  	srcInfo, _ := src.Stat()
  	obj.PutContent("MyAccount", "MyBucket", "MyObjectInTheBucket", srcInfo.Size(), src)
  }
*/
package oio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Returned when the target resource is not found, for any reason. E.g. if the
// resource is an object in a container, ErrorNotFound can be returned if the
// Reference doesn't exist, or the container, or the object itself.
var ErrorNotFound = errors.New("Resource not found")

// The feature you are calling has not been implemented yet. This should not
// happen (at least for a long time)
var errorNotImplemented = errors.New("Not Yet Implemented")

// You are calling a Namespace which has not been configured
var ErrorNsNotManaged = errors.New("Namespace Not Managed")

// The configuration value has not been provided.
var ErrorConfiguration = errors.New("Invalid configuration")

// A prefix to all the headers related to chunk attributes
const RAWX_HEADER_PREFIX = "X-oio-chunk-meta-"

const (
	KeyProxy      = "proxy"
	KeyAutocreate = "autocreate"
)

// Minimal interface for a configuration set
type Config interface {
	// Get the raw value to the given key, in the given namespace.
	GetString(ns, key string) (string, error)
	// Wrap GetString() and checks the value represents a boolean
	GetBool(ns, key string) (bool, error)
}

// A service item managed by the OpenIO SDS's directory services.
type Service struct {
	Seq  uint64 `json:"seq"`
	Type string `json:"type"`
	Url  string `json:"host"`
	Args string `json:"args"`
}

// Used in several places (container, objects), the Property is used to
// store sets of <key,value> pairs associated to entities.
type Property struct {
	Key   string
	Value string
}

// A handy structure gathering all the information that can be collected
// at once from the directory service.
type RefDump struct {
	Directory  []Service  `json:"dir"`
	Services   []Service  `json:"srv"`
	Properties []Property `json:"props,omitempty"`
}

type Chunk struct {
	Url      string `json:"url"`
	Position string `json:"pos"`
	Size     uint64 `json:"size"`
	Hash     string `json:"hash"`
}

type Content struct {
	Name           string `json:"name"`
	Version        uint64 `json:"ver"`
	Size           uint64 `json:"size"`
	CTime          uint64 `json:"ctime"`
	Deleted        bool   `json:"deleted,omitempty"`
	Hash           string `json:"hash"`
	Policy         string `json:"policy"`
	SystemMetadata string `json:"system_metadata"`
}

// ContainerListing is a handy structure to gather all the output
// generated when ListContents() is called on the containers service
type ContainerListing struct {
	Objects    []Content
	Properties []Property
}

// Client to the directory services of the Software Defined Storage.
type Directory interface {

	// Check the existence of a reference.
	HasReference(account, reference string) (bool, error)

	// Create the reference in the direcory. It returns if something has been
	// created or not: if the output is (false,nil) then the container already
	// existed.
	CreateReference(account, reference string) (bool, error)

	DeleteReference(account, reference string) (bool, error)
	DumpReference(account, reference string) (RefDump, error)

	LinkServices(account, reference, srvtype string) ([]Service, error)
	RenewServices(account, reference, srvtype string) ([]Service, error)
	ForceServices(account, reference string, srv []Service) ([]Service, error)
	ListServices(account, reference, srvtype string) ([]Service, error)
	UnlinkServices(account, reference, srvtype string) (bool, error)

	GetProperties(account, reference string) (map[string]string, error)
	SetProperties(account, reference string, props map[string]string) (bool, error)
	DeleteProperties(account, reference string, keys []string) (bool, error)
}

type Container interface {
	CreateContainer(account, reference string) (bool, error)
	DeleteContainer(account, reference string) (bool, error)
	HasContainer(account, reference string) (bool, error)

	ListContents(account, reference string) (ContainerListing, error)
	GetContent(account, reference, path string) ([]Chunk, []Property, error)
	GenerateContent(account, reference, path string, size uint64) ([]Chunk, error)
	PutContent(account, reference, path string, size uint64, chunks []Chunk) error
	DeleteContent(account, reference, path string) (bool, error)
}

type ObjectStorage interface {
	PutContent(account, reference, path string, size uint64, in io.ReadSeeker) error
	GetContent(account, reference, path string) (io.ReadCloser, error)
	DeleteContent(account, reference, path string) error
}

//------------------------------------------------------------------------------

type StaticConfig struct {
	pairs map[string]string
}

func (cfg *StaticConfig) Set(ns, key, value string) {
	k := strings.Join([]string{ns, key}, "/")
	cfg.pairs[k] = value
}

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

func makeHttpClient(ns string, cfg Config) *http.Client {
	dial := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 1000*time.Millisecond)
	}
	transport := http.Transport{Dial: dial}
	return &http.Client{Transport: &transport}
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

//------------------------------------------------------------------------------

type objectStorageClient struct {
	directory Directory
	container Container
}

func MakeDefaultObjectStorageClient(ns string, cfg Config) (ObjectStorage, error) {
	d, _ := MakeDirectoryClient(ns, cfg)
	c, _ := MakeContainerClient(ns, cfg)
	return MakeObjectStorageClient(d, c)
}

func MakeObjectStorageClient(d Directory, c Container) (ObjectStorage, error) {
	out := &objectStorageClient{directory: d, container: c}
	return out, nil
}

func (cli *objectStorageClient) DeleteContent(account, reference, path string) error {
	_, err := cli.container.DeleteContent(account, reference, path)
	return err
}

func (cli *objectStorageClient) GetContent(account, reference, path string) (io.ReadCloser, error) {
	chunks, _, err := cli.container.GetContent(account, reference, path)
	if err != nil {
		return nil, err
	}
	return makeChunksDownload(chunks)
}

func (cli *objectStorageClient) PutContent(account, reference, path string, size uint64, src io.ReadSeeker) error {
	if src == nil {
		panic("Invalid input")
	}
	var err error

	// Get a list of chunks
	chunks, err := cli.container.GenerateContent(account, reference, path, size)
	if err != nil {
		return err
	}

	mcSet, err := organizeChunks(chunks)
	if err != nil {
		return err
	}
	// Alert if RAIN is used (NYI)
	for _, mc := range mcSet {
		if len(mc.parity) > 0 {
			return errors.New("Erasure coding not yet implemented")
		}
	}

	// Patch the chunks'es size
	// TODO get the chunk_size from somewhere reliable (i.e. the config)
	var offset uint64 = 0
	chunk_size := maxSize(&chunks)
	remaining := size
	for i, _ := range mcSet {
		mc := &(mcSet[i])
		mc.meta_size = decRet(&remaining, chunk_size)
		mc.offset = offset
		offset = offset + mc.meta_size
		for ii, _ := range (*mc).data {
			(*mc).data[ii].Size = mc.meta_size
		}
	}

	// upload each meta-chunk
	for i, _ := range mcSet {
		mc := &(mcSet[i])
		pp := makePolyPut()
		for _, chunk := range chunks {
			pp.addTarget(chunk.Url)
		}
		pp.addHeader("X-oio-req-id", "0")
		pp.addHeader(RAWX_HEADER_PREFIX+"container-id", "0000000000000000000000000000000000000000000000000000000000000000")
		pp.addHeader(RAWX_HEADER_PREFIX+"content-path", path)
		pp.addHeader(RAWX_HEADER_PREFIX+"content-size", strconv.FormatUint(size, 10))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-chunksnb", strconv.Itoa(len(mcSet)))
		pp.addHeader(RAWX_HEADER_PREFIX+"content-metadata-sys", "")
		pp.addHeader(RAWX_HEADER_PREFIX+"chunk-id", "0000000000000000000000000000000000000000000000000000000000000000")
		pp.addHeader(RAWX_HEADER_PREFIX+"chunk-pos", strconv.Itoa(i))
		r := makeSliceReader(src, mc.meta_size)
		err = pp.do(&r)
		if err != nil {
			return err
		}
	}

	err = cli.container.PutContent(account, reference, path, size, chunks)
	if err != nil {
		return err
	}
	return nil
}

//------------------------------------------------------------------------------

type containerClient struct {
	ns     string
	config Config
}

func (cli *containerClient) simpleRequest(req *http.Request) (bool, error) {
	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return false, err
	}

	if rep.StatusCode/100 == 2 {
		return true, nil
	} else if rep.StatusCode == 404 {
		return false, ErrorNotFound
	} else {
		return false, errors.New("HTTP error")
	}
}

func (cli *containerClient) actionFlags(force bool) string {
	var tokens []string = make([]string, 0)
	if ok, _ := cli.config.GetBool(cli.ns, KeyAutocreate); ok {
		tokens = append(tokens, "autocreate")
	}
	if force {
		tokens = append(tokens, "force")
	}
	return strings.Join(tokens, ", ")
}

func MakeContainerClient(ns string, cfg Config) (Container, error) {
	out := &containerClient{ns: ns, config: cfg}
	return out, nil
}

func (cli *containerClient) getRefUrl(account, reference string) string {
	return fmt.Sprintf("http://%s/v2.0/m2/%s/%s/%s", getProxyUrl(cli.ns, cli.config),
		cli.ns, account, reference)
}

func (cli *containerClient) getContentUrl(account, reference, path string) string {
	u := cli.getRefUrl(account, reference)
	return u + "/" + path
}

func (cli *containerClient) CreateContainer(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("PUT", url, nil)
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.simpleRequest(req)
}

func (cli *containerClient) DeleteContainer(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *containerClient) HasContainer(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("HEAD", url, nil)
	ok, err := cli.simpleRequest(req)
	if err == ErrorNotFound {
		return false, nil
	}
	return ok, err
}

func (cli *containerClient) ListContents(account, reference string) (ContainerListing, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("GET", url, nil)
	out := ContainerListing{Objects: make([]Content, 0), Properties: make([]Property, 0)}

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return out, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&out)
		return out, err
	} else if rep.StatusCode == 404 {
		return out, ErrorNotFound
	} else {
		return out, errors.New("HTTP error")
	}
}

func (cli *containerClient) GetContent(account, reference, path string) ([]Chunk, []Property, error) {
	url := cli.getContentUrl(account, reference, path)
	req, _ := http.NewRequest("GET", url, nil)

	chunks := make([]Chunk, 0)
	props := make([]Property, 0)

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return chunks, props, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&chunks)
		return chunks, props, err
	} else if rep.StatusCode == 404 {
		return chunks, props, ErrorNotFound
	} else {
		return chunks, props, errors.New("HTTP error")
	}
}

func (cli *containerClient) GenerateContent(account, reference, path string, size uint64) ([]Chunk, error) {
	url := cli.getContentUrl(account, reference, path)

	args := map[string]string{"policy": "", "size": strconv.FormatUint(size, 10)}
	encoded, _ := json.Marshal(args)
	body := fmt.Sprintf("{\"action\":\"Beans\",\"args\":%s}", string(encoded))

	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader(body))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))

	chunks := make([]Chunk, 0)

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return chunks, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&chunks)
		return chunks, err
	} else if rep.StatusCode == 404 {
		return chunks, ErrorNotFound
	} else {
		return chunks, errors.New("HTTP error")
	}
}

func (cli *containerClient) PutContent(account, reference, path string, size uint64, chunks []Chunk) error {
	url := cli.getContentUrl(account, reference, path)

	body, _ := json.Marshal(chunks)

	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	req.Header.Set("X-oio-content-meta-length", strconv.FormatUint(size, 10))

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return err
	}

	if rep.StatusCode/100 == 2 {
		return nil
	} else if rep.StatusCode == 404 {
		return ErrorNotFound
	} else {
		return errors.New("HTTP error")
	}
}

func (cli *containerClient) DeleteContent(account, reference, path string) (bool, error) {
	url := cli.getContentUrl(account, reference, path)
	req, _ := http.NewRequest("DELETE", url, nil)

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return false, err
	}

	if rep.StatusCode/100 == 2 {
		return true, nil
	} else if rep.StatusCode == 404 {
		return false, ErrorNotFound
	} else {
		return false, errors.New("HTTP error")
	}
}

//------------------------------------------------------------------------------

type directoryClient struct {
	config Config
	ns     string
}

func MakeDirectoryClient(ns string, cfg Config) (Directory, error) {
	out := &directoryClient{ns: ns, config: cfg}
	return out, nil
}

func (cli *directoryClient) actionFlags(force bool) string {
	var tokens []string = make([]string, 0)
	if ok, _ := cli.config.GetBool(cli.ns, KeyAutocreate); ok {
		tokens = append(tokens, "autocreate")
	}
	if force {
		tokens = append(tokens, "force")
	}
	return strings.Join(tokens, ", ")
}

func (cli *directoryClient) serviceRequest(req *http.Request) ([]Service, error) {
	var tab []Service = make([]Service, 0)

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return tab, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&tab)
		return tab, err
	} else if rep.StatusCode == 404 {
		return tab, ErrorNotFound
	} else {
		return tab, errors.New("HTTP error")
	}
}

func (cli *directoryClient) simpleRequest(req *http.Request) (bool, error) {
	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return false, err
	}

	if rep.StatusCode/100 == 2 {
		return true, nil
	} else if rep.StatusCode == 404 {
		return false, ErrorNotFound
	} else {
		return false, errors.New("HTTP error")
	}
}

func (cli *directoryClient) getRefUrl(account, reference string) string {
	return fmt.Sprintf("http://%s/v2.0/dir/%s/%s/%s", getProxyUrl(cli.ns, cli.config),
		cli.ns, account, reference)
}

func (cli *directoryClient) getTypeUrl(account, reference, srvtype string) string {
	return fmt.Sprintf("http://%s/v2.0/dir/%s/%s/%s/%s",
		getProxyUrl(cli.ns, cli.config), cli.ns, account, reference, srvtype)
}

func (cli *directoryClient) HasReference(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("HEAD", url, nil)
	ok, err := cli.simpleRequest(req)
	if err == ErrorNotFound {
		return false, nil
	}
	return ok, err
}

func (cli *directoryClient) CreateReference(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("PUT", url, nil)
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteReference(account, reference string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DumpReference(account, reference string) (RefDump, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("GET", url, nil)
	tmp := RefDump{make([]Service, 0), make([]Service, 0), make([]Property, 0)}

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return tmp, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&tmp)
		return tmp, err
	} else if rep.StatusCode == 404 {
		return tmp, ErrorNotFound
	} else {
		return tmp, errors.New("HTTP error")
	}
}

func (cli *directoryClient) ListServices(account, reference, srvtype string) ([]Service, error) {
	url := cli.getTypeUrl(account, reference, srvtype)
	req, _ := http.NewRequest("GET", url, nil)
	return cli.serviceRequest(req)
}

func (cli *directoryClient) LinkServices(account, reference, srvtype string) ([]Service, error) {
	url := cli.getTypeUrl(account, reference, srvtype)
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader("{\"action\":\"Link\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) RenewServices(account, reference, srvtype string) ([]Service, error) {
	url := cli.getTypeUrl(account, reference, srvtype)
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader("{\"action\":\"Renew\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) ForceServices(account, reference string, srv []Service) ([]Service, error) {
	var srvtype string = srv[0].Type
	url := cli.getTypeUrl(account, reference, srvtype)
	body, _ := json.Marshal(srv)
	req, _ := http.NewRequest("POST", url+"/action", bytes.NewBuffer(body))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) UnlinkServices(account, reference, srvtype string) (bool, error) {
	url := cli.getTypeUrl(account, reference, srvtype)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) GetProperties(account, reference string) (map[string]string, error) {
	url := cli.getRefUrl(account, reference)
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader("{\"action\":\"GetProperties\",\"args\":null}"))
	var tab map[string]string = make(map[string]string)

	p := makeHttpClient(cli.ns, cli.config)
	rep, err := p.Do(req)
	if rep != nil {
		defer rep.Body.Close()
	}
	if err != nil {
		return tab, err
	}

	if rep.StatusCode/100 == 2 {
		decoder := json.NewDecoder(rep.Body)
		err = decoder.Decode(&tab)
		return tab, err
	} else if rep.StatusCode == 404 {
		return tab, ErrorNotFound
	} else {
		return tab, errors.New("HTTP error")
	}
}

func (cli *directoryClient) SetProperties(account, reference string, props map[string]string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	encoded, _ := json.Marshal(props)
	body := fmt.Sprintf("{\"action\":\"SetProperties\",\"args\":%s}", string(encoded))
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader(body))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteProperties(account, reference string, keys []string) (bool, error) {
	url := cli.getRefUrl(account, reference)
	encoded, _ := json.Marshal(keys)
	body := fmt.Sprintf("{\"action\":\"DeleteProperties\",\"args\":%s}", string(encoded))
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader(body))
	return cli.simpleRequest(req)
}
