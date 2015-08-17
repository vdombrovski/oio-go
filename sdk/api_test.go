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
	"encoding/hex"
	"log"
	"strings"
	"testing"
)

func TestHash(t *testing.T) {
	n := FlatName{N: "NS", A: "", U: "JFS"}
	var hexa string = hex.EncodeToString(ComputeUserId(&n))
	hexa = strings.ToUpper(hexa)
	if hexa != "C3F36084054557E6DBA6F001C41DAFBFEF50FCC83DB2B3F782AE414A07BB3A7A" {
		log.Fatal("ID mismatch")
	}
}
