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
	// version
	V int64
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

// Returns the version of the object
func (n *FlatName) Version() int64 { return n.V }
