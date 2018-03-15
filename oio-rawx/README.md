# OpenIO SDS RAWX

OpenIO RAWX's Golang implementation.

Licensed under the terms of AGPLv3

### Features

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

### Multi-filerepo TODO:
  * [x] Working GET, PUT, DELETE
  * [x] Working mover
  * [ ] Working HEAD
  * [x] Harmonize: support single filerepo
  * [x] Harmonize: old arguments and conf file
  * [ ] Code cleanup
  * [ ] Deadlock tests
  * [ ] Functional tests
  * [ ] (Optional) goroutine pool for the mover


### Install and test:

> Make sure your target FS supports xattrs

```sh
cd ~/go/src
git clone [this] .
cd oio-go/oio-rawx
go build -o rawx *.go
```

Now create one or more filerepos

```
fallocate -l 20M nvme.img
mkdir -p ~/mnt/nvme
sudo mkfs -t xfs nvme.img
sudo mount nvme.img #/mnt/nvme
mkdir -p ~/mnt/hdd
```

Now create a configuration file, see here for details:
https://github.com/open-io/oio-sds/blob/master/rawx-apache2/httpd.conf.sample

### Available config options:

- Listen: Address on which the rawx should listen
- grid_namespace: Namespace on which to operate
- grid_filerepos*: Comma-separated list of filerepos (no spaces)

\* This parameter is new and should be added anywhere in the file

### Run the rawx

./rawx -D FOREGROUND -f /etc/oio/sds/OPENIO/rawx-0/rawx-0-httpd.conf

More options will be supported in the future. The -D option is ignored, and is present for compatibility with the should
rawx.

### Cleanup the xattrs of a directory

```
apt install -y xattr
setfattr -x user.rawx_server.address ~/mnt/test
setfattr -x user.rawx_server.namespace ~/mnt/test
```
