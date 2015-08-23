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
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

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
		return false, readProxyError(rep.StatusCode, rep)
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

func (cli *containerClient) getRefUrl(n UserName) string {
	return fmt.Sprintf("http://%s/v2.0/m2/%s/%s/%s", getProxyContainerUrl(cli.ns, cli.config),
		cli.ns, n.Account(), n.User())
}

func (cli *containerClient) getContentUrl(n ObjectName) string {
	u := cli.getRefUrl(n)
	return u + "/" + n.Path()
}

func (cli *containerClient) CreateContainer(n ContainerName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("PUT", url, nil)
	req.Header.Set("X-oio-action-mode", cli.actionFlags(false))
	return cli.simpleRequest(req)
}

func (cli *containerClient) DeleteContainer(n ContainerName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
	req, _ := http.NewRequest("DELETE", url, nil)
	return cli.simpleRequest(req)
}

func (cli *containerClient) HasContainer(n ContainerName) (bool, error) {
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

func (cli *containerClient) ListContents(n ContainerName) (ContainerListing, error) {
	if n.NS() != cli.ns {
		var out ContainerListing
		return out, ErrorNsNotManaged
	}
	url := cli.getRefUrl(n)
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
		return out, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *containerClient) GetContent(n ObjectName) ([]Chunk, []Property, error) {
	chunks := make([]Chunk, 0)
	props := make([]Property, 0)

	if n.NS() != cli.ns {
		return chunks, props, ErrorNsNotManaged
	}
	url := cli.getContentUrl(n)
	req, _ := http.NewRequest("GET", url, nil)

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
		return chunks, props, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *containerClient) GenerateContent(n ObjectName, size uint64) ([]Chunk, error) {
	if n.NS() != cli.ns {
		return nil, ErrorNsNotManaged
	}
	url := cli.getContentUrl(n)

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
		return chunks, readProxyError(rep.StatusCode, rep)
	}
}

func (cli *containerClient) PutContent(n ObjectName, size uint64, chunks []Chunk) error {
	if n.NS() != cli.ns {
		return ErrorNsNotManaged
	}
	url := cli.getContentUrl(n)

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
		return readProxyError(rep.StatusCode, rep)
	}
}

func (cli *containerClient) DeleteContent(n ObjectName) (bool, error) {
	if n.NS() != cli.ns {
		return false, ErrorNsNotManaged
	}
	url := cli.getContentUrl(n)
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
		return false, readProxyError(rep.StatusCode, rep)
	}
}
