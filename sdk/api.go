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
	name := oio.FlatName{N:ns, A:"MyAccount", U:"MyUser", P:"MyObject"}
  	cfg := oio.MakeStaticConfig()
  	cfg.Set(ns, "proxy", "127.0.0.1:6002")
  	obj, _ := oio.MakeDefaultObjectStorageClient(ns, cfg)
  	src, _ := os.Open("/Path/to/a/file")
  	srcInfo, _ := src.Stat()
  	obj.PutContent(&name, srcInfo.Size(), src)
  }

OpenIO is a company providing open source software solutions.
"OpenIO SDS" manages object storage software for massive storage infrastructures.
It allows to easily build and run large scale storage platforms for all your apps.
Please find more information on our homepage at http://openio.io, or our github.com acccunt at https://github.com/open-io.
Please visit the the wiki at https://github.com/open-io/oio-sds/wiki for an extensive description.

Quickly, you need to know that our object storage solutions manage several entities...
A "namespace" names a physical platform hosting several services.
An "account" is an entity used to virtually partition the namespace into logical units.
All the accounts on a same namespace share the same platform, but have their own configuration and quotas.
A "user" represents the end user managed by an account. An account might have (really) end users.
A "container" is a collection of contents and properties. Containers are almost identical to Amazon S3's buckets.
Eventually, the "contents" are objects (a.k.a. blobs, a.k.a. data without any expectation on the format) stored in containers.

OpenIO SDS is composed of distributed entities:
a "conscience" service responsible for the load-balancing among services,
a "directory" for data-bound services,
several kinds of "data-bound services" (and containers are such data that services bound to)
and "stateless services".

This SDK will present interfaces to thoses services.

*/
package oio

import (
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// Returned when the target resource is not found, for any reason. E.g. if the
// resource is an object in a container, ErrorNotFound can be returned if the
// User doesn't exist, or the container, or the object itself.
var ErrorNotFound = errors.New("Resource not found")

// The feature you are calling has not been implemented yet. This should not
// happen (at least for a long time)
var errorNotImplemented = errors.New("Not Yet Implemented")

// You are calling a Namespace which has not been configured
var ErrorNsNotManaged = errors.New("Namespace Not Managed")

// The configuration value has not been provided.
var ErrorConfiguration = errors.New("Invalid configuration")

var zeroByte = make([]byte, 1, 1)

// A prefix to all the headers related to chunk attributes
const RAWX_HEADER_PREFIX = "X-oio-chunk-meta-"

const (
	KeyProxyConscience = "proxy-conscience"
	KeyProxyContainer  = "proxy-container"
	KeyProxyDirectory  = "proxy-dir"
	KeyProxy           = "proxy"
	KeyAutocreate      = "autocreate"
)

// AccountName describes a set of getters for all the fields that uniquely
// identify an account (i.e. a namespace and the account name).
type AccountName interface {

	// Must returns the namespace's name (not empty)
	NS() string

	// Must return the account's name (not empty)
	Account() string
}

// UserName describes a set of getters for all the fields that uniquely
// identify an end-user (i.e. it extends AccountName with a user name).
type UserName interface {
	AccountName
	// Must return the user name (not empty)
	User() string
}

// ContainerName describes a set of getters for all the fields that uniquely
// identify a user's container (i.e. it extends UserName with an optional
// service subtype).
type ContainerName interface {
	UserName

	// Must return the service subtype (leave empty for unset)
	Type() string
}

// ObjectName describes a set of getters for all the fields that uniquely
// identify an object in a container (i.e. it extends ContainerName with a
// mandatory path and an optional version).
type ObjectName interface {
	ContainerName

	// Must return the object path (not empty)
	Path() string

	// Must return the object revision (leave 0 for the latest)
	Version() int64
}

// Minimal interface for a configuration set
type Config interface {

	// Get the raw value to the given key, in the given namespace.
	GetString(ns, key string) (string, error)

	// Wrap GetString() and checks the value represents a boolean
	GetBool(ns, key string) (bool, error)

	// Wrap GetString() and checks the value represents an integer
	GetInt(ns, key string) (int64, error)
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

// Chunks are objects parts. Objects are chunked to ensure a content deposit won't ever suffer from a full filesystem, but only a full namespace.
type Chunk struct {
	Url      string `json:"url"`
	Position string `json:"pos"`
	Size     uint64 `json:"size"`
	Hash     string `json:"hash"`
}

// A Content represents the minimal information stored in a container about a stored object.
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

	// Check the existence of an end user.
	HasUser(n UserName) (bool, error)

	// Create the end user in the direcory. It returns if something has been
	// created or not: if the output is (false,nil) then the container already
	// existed.
	CreateUser(n UserName) (bool, error)

	// Deletes the given end user. This will fail if the user is still linked
	// to services or still carries properties. It might return (false,nil)
	// if nothing was deleted (i.e. the container didn't exist).
	DeleteUser(n UserName) (bool, error)

	// Ask the directory a dump of the services and properties linked with the
	// end user.
	DumpUser(n UserName) (RefDump, error)

	// Bind a user to a service of a given kind. The service will be polled by
	// the directory service itself, using the conscience. If a service is
	// already bound, the directory service will return this. Note that this
	// is not guaranteed, since there is a check performed on the service: if
	// it is still available, OK; but if it is down the directory service might
	// be configured to replace it by an other instance of the same type.
	LinkServices(n UserName, srvtype string) ([]Service, error)

	// Acts as LinkServices() but assumes the current service (if any) is down.
	RenewServices(n UserName, srvtype string) ([]Service, error)

	// Bind the given service to the user. All the services in <srv> must
	// carry the same sequence number or the behavior is unknown.
	ForceServices(n UserName, srv []Service) ([]Service, error)

	// Get a list of all the services of the given type, bound to the given
	// service.
	ListServices(n UserName, srvtype string) ([]Service, error)

	// Dissociates the given user with all the services of the given type
	UnlinkServices(n UserName, srvtype string) (bool, error)

	// Get all the properties associated with the given user.
	GetAllProperties(n UserName) (map[string]string, error)

	// Bind an additional set of key/value pairs to the given user. If some
	// keys were already bound, they are replaced.
	SetProperties(n UserName, props map[string]string) (bool, error)

	// Remove some key/value bindings for the given user.
	DeleteProperties(n UserName, keys []string) (bool, error)
}

type Container interface {

	// Create the given container. Returns false if nothing was created (i.e.
	// (false,nil) means the container already exists)
	CreateContainer(n ContainerName) (bool, error)

	// Removes the container from the storage. Returns (true,nil) if the deletion
	// actually happened. Returns false if not. (false,nil) means the container
	//didn't exist yet.
	DeleteContainer(n ContainerName) (bool, error)

	// Check the container exists. (false,nil) means it doesn't. (false,!nil)
	// means we weren't able to check.
	HasContainer(n ContainerName) (bool, error)

	// Get a list of all the contents of the container.
	ListContents(n ContainerName) (ContainerListing, error)

	// Get a description of the content whos ename is given
	GetContent(n ObjectName) ([]Chunk, []Property, error)

	// Get places to upload a content with the given name and size
	GenerateContent(n ObjectName, size uint64) ([]Chunk, error)

	// Save the places used by the content with the given name and size
	PutContent(n ObjectName, size uint64, chunks []Chunk) error

	// Remove the given content from the storage
	DeleteContent(n ObjectName) (bool, error)
}

type ObjectStorage interface {

	// Uploads <size> bytes from <in> as an object named <n>.
	PutContent(n ObjectName, size uint64, in io.ReadSeeker) error

	// Get a stream to read the content.
	// TODO: make the output a "ReadSeekCloser" to let the appication efficiently
	// read a slice of it.
	GetContent(n ObjectName) (io.ReadCloser, error)

	// Remove the given content from the storage
	DeleteContent(n ObjectName) error
}

func makeHttpClient(ns string, cfg Config) *http.Client {
	dial := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 1000*time.Millisecond)
	}
	transport := http.Transport{Dial: dial}
	return &http.Client{Transport: &transport}
}

// Compute a unique ID for the given user name. This ID is internally used
// for sharding purposes.
func ComputeUserId(name UserName) []byte {
	h := sha256.New()
	out := make([]byte, 0, h.Size())
	if acct := name.Account(); len(acct) > 0 {
		h.Write([]byte(name.Account()))
		h.Write(zeroByte)
	}
	h.Write([]byte(name.User()))
	return h.Sum(out)
}

// Creates an implementation of an ObjectStorage, relying on the default
// implementation of a Directory and a Container clients.
func MakeDefaultObjectStorageClient(ns string, cfg Config) (ObjectStorage, error) {
	d, _ := MakeDirectoryClient(ns, cfg)
	c, _ := MakeContainerClient(ns, cfg)
	return MakeObjectStorageClient(d, c)
}

// Creates the default implementation for an ObjectStorage, relying on the given
// Directory and a Container implementations.
func MakeObjectStorageClient(d Directory, c Container) (ObjectStorage, error) {
	out := &objectStorageClient{directory: d, container: c}
	return out, nil
}

// Creates an instance of the default implementation of the Directory client
// implementation. The subsequent calls will only accept to serve the namespace
// now given, all other namespaces will result in the error ErrorNsNotManaged
// to be returned.
func MakeDirectoryClient(ns string, cfg Config) (Directory, error) {
	out := &directoryClient{ns: ns, config: cfg}
	return out, nil
}

// Creates an instance of the default implementation for the Container client
// interface. The output will only serve the given namespace, all the calls
// toward an other namesapce will result in an error.
func MakeContainerClient(ns string, cfg Config) (Container, error) {
	out := &containerClient{ns: ns, config: cfg}
	return out, nil
}
