/*
Package pipeline implements a low-level pipelined API for SLO uploads.

Most of the functions defined in this package are stages in an upload
pipeline that communicate with channels of type FileChunk. A FileChunk
is a representation of a chunk of a file that is being uploaded, but is
also how we represent SLO manifests before uploading them.

To use the pipeline, either start with the BuildChunks source that creates
a channel of FileChunks or make your own data source. Pass channels of
FileChunks to each stage, and use the return value of one stage as input
to the next.

The API expects an errors channel to be passed to most stages that will
allow it to report nonfatal errors. It is generally sufficient to create
a single errors channel and pass it to all stages. Ensure that you drain
the errors channel though, or your pipeline will block on the first error
that it encounters.
*/
package pipeline
