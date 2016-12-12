/*
Package swiftlygo makes working with Large Objects in OpenStack Object Storage easier.

The auth subpackage provides a convenient abstraction for talking to an
Object Storage instance. See the documentation of the auth package for details
on how to authenticate with Object Storage Instances.
The pipeline subpackage implements a low-level API for Static Large Object creation if
the SloUploader doesn't offer the level of control that your application requires.

The root swiftlygo package provides functionality for easily creating Dynamic
Large Objects and Static Large Objects.

Both the SloUploader and DloUploader types are easy to use. They only have one method,
Upload(), that performs a synchronous upload (it will only return after the upload is
complete). The SloUploader also exposes a Status struct that can be used during an
upload to query the progress up the upload.
*/
package swiftlygo
