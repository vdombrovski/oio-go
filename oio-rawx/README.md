# OpenIO SDS RAWX

OpenIO RAWX's Golang implementation.

Licensed under the terms of AGPLv3

## Features

  * [ ] metadata modification (POST? PROPPATCH?)
  * [ ] Partial GET (Range header not considered at all)
  * [ ] .pending management
  * [ ] Access log in a format compliant with the other OpenIO services
  * [x] /stat request handling
  * [x] Compression to be made.
  * [x] xattr-lock of the volume
  * [x] GET management with xattr returned in attr headers
  * [x] PUT management with attr headers saved in xattr
  * [x] DELETE management
  * [x] Alternative names management
  * [x] Chunks hashed path
  * [x] MD5 computation of the DATA put returned in the right header

