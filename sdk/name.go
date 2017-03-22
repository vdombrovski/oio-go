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

// FlatName implements the interface ObjectName thus also all of its parents:
// AccountName, UserName, ContainerName.
type FlatName struct {
	// namespace
	N string
	// account
	A string
	// user
	U string
	// service subtype
	S string
	// path
	P string
	// id
	I string
	// version
	V uint64
}

type fullyQualifiedContent struct {
	content   *Content
	container ContainerName
}

// Returns the namespace's name
func (n *FlatName) NS() string { return n.N }

// Returns the Account's name
func (n *FlatName) Account() string { return n.A }

// Returns the user's name
func (n *FlatName) User() string { return n.U }

// Returns the optional service subtype
func (n *FlatName) Type() string { return n.S }

// Returns the name of the object in the container
func (n *FlatName) Path() string { return n.P }

// Returns the name of the object in the container
func (n *FlatName) Id() string { return n.I }

// Returns the version of the object
func (n *FlatName) Version() uint64 { return n.V }

func (self *fullyQualifiedContent) NS() string { return self.container.NS() }

func (self *fullyQualifiedContent) Account() string { return self.container.Account() }

func (self *fullyQualifiedContent) User() string { return self.container.User() }

func (self *fullyQualifiedContent) Type() string { return self.container.Type() }

func (self *fullyQualifiedContent) Path() string { return self.content.Header.Name }

func (self *fullyQualifiedContent) Id() string { return self.content.Header.Id }

func (self *fullyQualifiedContent) Version() uint64 { return self.content.Header.Version }
