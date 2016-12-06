# swiftlygo


[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![Build Status](https://travis-ci.org/ibmjstart/swiftlygo.svg?branch=master)](https://travis-ci.org/ibmjstart/swiftlygo)
[![GoDoc](https://godoc.org/github.com/ibmjstart/swiftlygo?status.svg)](https://godoc.org/github.com/ibmjstart/swiftlygo)

> Golang library for creating Static and Dynamic Large Objects in OpenStack Swift

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Background

OpenStack Object Storage has the ability to create two powerful kinds of aggregate file: Static and Dynamic
Large Objects.

A Static Large Object (SLO) is a manifest file that lists other files within an object storage
instance in a particular order. When you read the contents of an SLO, you get the contents of every file listed
within its manifest in the order that they are defined within the manifest. This allows you to build large files
out of small pieces. OpenStack Object Storage does not allow single files to be larger than 5GB, but an SLO
manifest can reference many files of size <= 5GB to create a single readable file within the Object Store that is
<= 5TB in size. This can be immensely helpful for large datasets.

A Dynamic Large Object (DLO) is equally useful, but in different circumstances. Rather than specify an ordered
list of files to be treated as the DLO's contents, a DLO specifies a filename prefix within a particular
Object Storage container. All files matching the prefix are treated as the contents of that DLO (sorted
lexographically). Since any files that match the prefix are considered part of the DLO, you can insert data
into DLOs after you create them by adding a file with the correct prefix at the correct sort order position.

Sadly, these two incredible features of the OpenStack Object Store are difficult to access and use. We wrote
this library to make creating these Large Object files easier.

## Install

To download the package, run
```
go get github.com/ibmjstart/swiftlygo
```

To use it within your code, add
```
import "github.com/ibmjstart/swiftlygo"
```

## Usage

`swiftlygo` has two main sets of functionality: creating SLOs and DLOs. The API for each is slightly different, since each requires different information.

Both APIs rely on the `auth.Destination` interface defined in the `auth` subpackage.

The sections below give an overview of each API. For more thorough docs, use
```
godoc github.com/ibmjstart/swiftlygo
godoc github.com/ibmjstart/swiftlygo/auth
godoc github.com/ibmjstart/swiftlygo/slo
```

### SLOs

The API for creating SLOs is based around uploading a single large file. That file will be broken into
chunks, and each chunk will be uploaded to Object Storage as a separate file. After all of the chunks
have been uploaded, the SLO manifest file will be uploaded. If this process is completed successfully,
You will be able to reference the entire file by the name of the manifest.

If your upload is interrupted, you can ensure that the boolean `onlyMissing` parameter to `slo.NewUploader` is set to `true`, which will skip all uploads for which the files are already present within the targeted object storage container. This can save a lot of time if you were most of the way through a previous upload.

Here's a simple example of using the SLO API to upload a file.
```go
package example

import (
	"github.com/ibmjstart/swiftlygo/auth"
	"github.com/ibmjstart/swiftlygo/slo"
	"os"
)

func main() {
	//using object storage credentials from VCAP.json or similar
	destination, err := auth.Authenticate("username", "apikey", "authurl", "domain", "tenant")
	if err != nil {
		// connection failed, handle appropriately
	}
	uploadFile, err := os.Open("file/path")
	if err != nil {
		// reading file failed, handle appropriately
	}
	uploader, err := slo.NewUploader(destination,
		10000000,//file chunk size in bytes (set to something larger for multi-gigabyte files and something smaller for files < 10MB)
		"container name",
		"object name",//name that you want to reference the whole SLO by
		uploadFile,
		8,//maxiumum number of parallel uploads allowed
		true,//upload only the file chunks that have not already been uploaded
		os.Stdout)
	if err != nil {
		// there was an error preparing the upload, handle appropriately
	}
	err = uploader.Upload()
	if err != nil {
		// there was an error uploading your SLO, handle appropriately
	}
}
```

### DLOs

DLOs are slightly different from SLOs in that they allow their segments to be uploaded independently from the 
manifest file. DLO manifests have an attribute that defines a container and prefix. Any files in this container 
with the specified prefix will become segments of the DLO, regardless of whether they were uploaded before, after 
or at the same time as the manifest. All files meeting these criteria will be downloaded as one file (composed in
lexicographical order) when the DLO is downloaded.

Here's an example of using the DLO API to create a manifest.
```go
package example

import (
	"github.com/ibmjstart/swiftlygo"
	"github.com/ibmjstart/swiftlygo/auth"
)

func main() {
	//using object storage credentials from VCAP.json or similar
	destination, err := auth.Authenticate("username", "apikey", "authurl", "domain", "tenant")
	if err != nil {
		// connection failed, handle appropriately
	}
	uploader := swiftlygo.NewDloManifestUploader(destination,
		"dlo container name", //name of the container the manifest will be created in
		"manifest name",
		"object container", //name of the container the DLO's segments will be in
		"prefix-")          //prefix for files that are segments of this DLO
	if err != nil {
		// there was an error preparing the upload, handle appropriately
	}
	err = uploader.Upload()
	if err != nil {
		// there was an error uploading your DLO, handle appropriately
	}
}
```

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License
Apache 2.0
 © IBM jStart
