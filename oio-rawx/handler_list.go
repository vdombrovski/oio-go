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
	"fmt"
	"net/http"
)

type listHandler struct {
	rawx *rawxService
}

func doGetList(rr *rawxRequest) {
	rr.replyCode(http.StatusOK)
	for i := 0; i < 10; i++ {
		rr.rep.Write([]byte(fmt.Sprintf("%p\n", rr)))
	}
}

func (self *listHandler) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	self.rawx.serveHTTP(rep, req, func(rr *rawxRequest) {
		switch req.Method {
		case "GET":
			doGetList(rr)
		default:
			rr.replyCode(http.StatusMethodNotAllowed)
		}
	})
}
