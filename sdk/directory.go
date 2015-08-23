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

func (cli *directoryClient) getRefUrl(n UserName) string {
	return fmt.Sprintf("http://%s/v2.0/dir/%s/%s/%s", getProxyDirectoryUrl(cli.ns, cli.config), cli.ns, n.Account(), n.User())
}

func (cli *directoryClient) getTypeUrl(n UserName, srvtype string) string {
	return fmt.Sprintf("http://%s/v2.0/dir/%s/%s/%s/%s", getProxyDirectoryUrl(cli.ns, cli.config), cli.ns, n.Account(), n.User(), srvtype)
}

func (cli *directoryClient) HasUser(n UserName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("HEAD", url, nil)
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
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("PUT", url, nil)
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteUser(n UserName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DumpUser(n UserName) (RefDump, error) {
	tmp := RefDump{make([]Service, 0), make([]Service, 0), make([]Property, 0)}
	if n.NS() != cli.ns {
		return tmp, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("GET", url, nil)

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
	url := cli.getTypeUrl(n, srvtype)
	req, _ := http.NewRequest("GET", url, nil)
	return cli.serviceRequest(req)
}

func (cli *directoryClient) LinkServices(n UserName, srvtype string) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	url := cli.getTypeUrl(n, srvtype)
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader("{\"action\":\"Link\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) RenewServices(n UserName, srvtype string) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	url := cli.getTypeUrl(n, srvtype)
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader("{\"action\":\"Renew\",\"args\":null}"))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) ForceServices(n UserName, srv []Service) ([]Service, error) {
	if n.NS() != cli.ns {
		return make([]Service, 0), ErrorNsNotManaged
	}
	var srvtype string = srv[0].Type
	url := cli.getTypeUrl(n, srvtype)
	body, _ := json.Marshal(srv)
	req, _ := http.NewRequest("POST", url+"/action", bytes.NewBuffer(body))
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.serviceRequest(req)
}

func (cli *directoryClient) UnlinkServices(n UserName, srvtype string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getTypeUrl(n, srvtype)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *directoryClient) GetAllProperties(n UserName) (map[string]string, error) {
	if n.NS() != cli.ns {
		return make(map[string]string), ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
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
		return tab, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *directoryClient) SetProperties(n UserName, props map[string]string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	encoded, _ := json.Marshal(props)
	body := fmt.Sprintf("{\"action\":\"SetProperties\",\"args\":%s}", string(encoded))
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader(body))
	return cli.simpleRequest(req)
}

func (cli *directoryClient) DeleteProperties(n UserName, keys []string) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	encoded, _ := json.Marshal(keys)
	body := fmt.Sprintf("{\"action\":\"DeleteProperties\",\"args\":%s}", string(encoded))
	req, _ := http.NewRequest("POST", url+"/action", strings.NewReader(body))
	return cli.simpleRequest(req)
}
