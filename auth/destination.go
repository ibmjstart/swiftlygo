package auth

import (
	"fmt"
	"github.com/ncw/swift"
	"io"
	"regexp"
	"strconv"
)

// Destination defines a valid upload destination for files.
type Destination interface {
	CreateFile(container string, objectName string, checkHash bool, Hash string) (io.WriteCloser, error)
	FileNames(container string) ([]string, error)
	AuthUrl() string
	AuthToken() string
}

// SwiftDestination implements the Destination interface for OpenStack Swift.
type SwiftDestination struct {
	SwiftConnection *swift.Connection
}

// CreateFile begins the process of creating a file in the destination. Write data to
// the returned WriteCloser and then close it to upload the data. Be sure to handle errors.
func (s *SwiftDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return s.SwiftConnection.ObjectCreate(container, objectName, checkHash, Hash, "", nil)
}

// FileNames returns a slice of the names of all files already in the destination container.
func (s *SwiftDestination) FileNames(container string) ([]string, error) {
	return s.SwiftConnection.ObjectNamesAll(container, nil)
}

// AuthUrl retrieves the Authentication URL for this destination.
func (s *SwiftDestination) AuthUrl() string {
	return s.SwiftConnection.StorageUrl
}

// AuthToken returns the authentication token for this destination.
func (s *SwiftDestination) AuthToken() string {
	return s.SwiftConnection.AuthToken
}

// GetAuthVersion extracts the OpenStack auth version from the end of an authURL.
func getAuthVersion(url string) (int, error) {
	// Extract auth version from auth URL
	authVersionRegex, err := regexp.Compile(".*/v([0-9])[.0-9]*/?$")
	if err != nil {
		return 0, fmt.Errorf("Unable to compile auth version regex")
	}
	matches := authVersionRegex.FindStringSubmatch(url)
	if len(matches) < 2 {
		return 0, fmt.Errorf("Unable to extract an auth version number from url %s", url)
	}
	authVersionNumber, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("Unable to convert version number %s to an integer", matches[1])
	}
	return authVersionNumber, nil
}

// authenticate logs in to OpenStack object storage and returns a connection to the
// object store. The url MUST have its auth version at the end: https://example.com/v{1,2,3}
func Authenticate(username, apiKey, authURL, domain, tenant string) (Destination, error) {
	version, err := getAuthVersion(authURL)
	if err != nil {
		return &SwiftDestination{}, err
	}
	connection := swift.Connection{
		UserName:    username,
		ApiKey:      apiKey,
		AuthUrl:     authURL,
		Domain:      domain,
		Tenant:      tenant,
		AuthVersion: version,
	}
	err = connection.Authenticate()
	if err != nil {
		return &SwiftDestination{SwiftConnection: &connection}, fmt.Errorf("Failed to authenticate with object storage: %s", err)
	}
	return &SwiftDestination{SwiftConnection: &connection}, nil
}
