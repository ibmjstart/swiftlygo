# swiftly-go
OpenStack Swift Protocol massive data uploader in Go

> To swiftly go where no data has gone before...

## To-Do's

- Manifest size pre-calculation and related warnings (manifest has max size)
- Option to upload only missing chunks of a file (easier retries)
- Accept VCAP.json instead of asking for each cred
- Upload progress bars
- Colorized output
- Convert to unsigned integers
- Check container & object existence before writing
- Time upload and provide info about current upload speed
- Automatic auth_url "/v3" suffixing
