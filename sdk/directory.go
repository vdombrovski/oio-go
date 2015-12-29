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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type proxyError struct {
	status  int    `json:"status"`
	message string `json:"message"`
}

func readProxyError(httpCode int, rep *http.Response) error {
	var pe proxyError
	decoder := json.NewDecoder(rep.Body)
	if err := decoder.Decode(&pe); err != nil {
		return errors.New(fmt.Sprintf("Proxy error: (%d) %s", httpCode, err.Error()))
	} else {
		return errors.New(fmt.Sprintf("Proxy error: (%d) (%d) %s", httpCode, pe.status, pe.message))
	}
}

type directoryClient struct {
	config Config
	ns     string
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
		return tab, readProxyError(rep.StatusCode, rep)
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
		err = readProxyError(rep.StatusCode, rep)
		return false, err
	}
}

func (cli *directoryClient) getRefUrl(n UserName, action string) string {
	return fmt.Sprintf("http://%s/v3.0/%s/reference/%s?acct=%s&ref=%s",
		getProxyDirectoryUrl(cli.ns, cli.config), cli.ns, action,
		url.QueryEscape(n.Account()), url.QueryEscape(n.User()))
}

func (cli *directoryClient) getTypeUrl(n UserName, action, srvtype string) string {
	return fmt.Sprintf("http://%s/v3.0/%s/reference/%s?acct=%s&ref=%s&type=%s",
		getProxyDirectoryUrl(cli.ns, cli.config), cli.ns, action,
		url.QueryEscape(n.Account()), url.QueryEscape(n.User()),
		url.QueryEscape(srvtype))
}

func (cli *directoryClient) HasUser(n UserName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	req, _ := http.NewRequest("GET", cli.getRefUrl(n, "show"), nil)
	ok, err := cli.simpleRequest(req)
	if err == ErrorNotFound {
		return false, nil
	}
	return ok, err
}

func (cli *directoryClient) CreateUser(n UserName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getRefUrl(n, "create"), nil)
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteUser(n UserName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getRefUrl(n, "destroy"), nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DumpUser(n UserName) (RefDump, error) {
	tmp := RefDump{make([]Service, 0), make([]Service, 0), make([]Property, 0)}
	if n.NS() != cli.ns {
		return tmp, ErrorNsNotManaged
	}

	req, _ := http.NewRequest("GET", cli.getRefUrl(n, "show"), nil)

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
		return tmp, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *directoryClient) ListServices(n UserName, srvtype string) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	req, _ := http.NewRequest("GET", cli.getTypeUrl(n, "show", srvtype), nil)
	return cli.serviceRequest(req)
}

func (cli *directoryClient) LinkServices(n UserName, srvtype string) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getTypeUrl(n, "link", srvtype),
		strings.NewReader("{\"action\":\"Link\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) RenewServices(n UserName, srvtype string) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getTypeUrl(n, "renew", srvtype),
		strings.NewReader("{\"action\":\"Renew\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) ForceServices(n UserName, srv []Service) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	var srvtype string = srv[0].Type
	body, _ := json.Marshal(srv)
	req, _ := http.NewRequest("POST", cli.getTypeUrl(n, "force", srvtype),
		bytes.NewBuffer(body))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) UnlinkServices(n UserName, srvtype string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getTypeUrl(n, "unlink", srvtype), nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) GetAllProperties(n UserName) (map[string]string, error) {
	if n.NS() != cli.ns {
		return make(map[string]string), ErrorNsNotManaged
	}
	req, _ := http.NewRequest("POST", cli.getRefUrl(n, "get_properties"), nil)
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
		return tab, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *directoryClient) SetProperties(n UserName, props map[string]string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	body, _ := json.Marshal(props)
	req, _ := http.NewRequest("POST", cli.getRefUrl(n, "set_properties"),
		bytes.NewBuffer(body))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteProperties(n UserName, keys []string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	body, _ := json.Marshal(keys)
	req, _ := http.NewRequest("POST", cli.getRefUrl(n, "del_properties"),
		bytes.NewBuffer(body))
	return cli.simpleRequest(req)
}
