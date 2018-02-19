# OpenIO SDS RAWX

OpenIO RAWX's Golang implementation.

Licensed under the terms of AGPLv3

## Features

  * [ ] Metadata modification
  * [ ] Compression of the chunks
  * [ ] Access log in a format compliant with the other OpenIO services
  * [x] Partial GET (Range header not considered at all)
  * [x] .pending management
  * [x] /stat request handling
  * [x] xattr-lock of the volume
  * [x] GET management with xattr returned in attr headers
  * [x] PUT management with attr headers saved in xattr
  * [x] DELETE management
  * [x] Alternative names management
  * [x] Chunks hashed path
  * [x] MD5 computation of the DATA put returned in the right header

## Install and test:

> Make sure your target FS supports xattrs

```sh
cd ~/go/src
git clone [this] .
cd oio-go/oio-rawx
go build -o rawx *.go
mkdir -p ~/mnt/test
./rawx $(realpath ~/mnt/test)
```

The rawx service will be listening on **127.0.0.1:5999**, on the namespace **OPENIO**, and use the repo at `~/mnt/test`.
See `./rawx -h` for more options.

## Cleanup the xattrs of a directory

```
apt install -y xattr
setfattr -x user.rawx_server.address ~/mnt/test
setfattr -x user.rawx_server.namespace ~/mnt/test
```
