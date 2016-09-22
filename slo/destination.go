package slo

import (
	"fmt"
	"github.com/ncw/swift"
	"regexp"
	"strconv"
)

// Destination wraps a swift connection with convenience methods
type Destination struct {
	connection *swift.Connection
}

// getAuthVersion extracts the OpenStack auth version from the end of an authURL.
func getAuthVersion(url string) (int, error) {
	// Extract auth version from auth URL
	authVersionRegex, err := regexp.Compile(".*/v([0-9])[.0-9]*/?$")
	if err != nil {
		return 0, fmt.Errorf("Unable to compile auth version regex: %s", err)
	}
	matches := authVersionRegex.FindStringSubmatch(url)
	if len(matches) < 2 {
		return 0, fmt.Errorf("Unable to extract an auth version number from url %s", url)
	}
	authVersionNumber, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("Unable to convert version number %s to an integer: %s", authVersionNumber, err)
	}
	return authVersionNumber, nil
}

// authenticate logs in to OpenStack object storage and returns a connection to the
// object store.
func authenticate(username, apiKey, authURL, domain, tenant string) (*swift.Connection, error) {
	version, err := getAuthVersion(authURL)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse auth URL: %s", err)
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
		return nil, fmt.Errorf("Failed to authenticate with object storage")
	}
	fmt.Println("Authenticated!")
	return &connection, err
}

// NewDestination creates a destination for data out of object storage credentials.
func NewDestination(username, apiKey, authURL, domain, tenant string) (*Destination, error) {
	connection, err := authenticate(username, apiKey, authURL, domain, tenant)
	if err != nil {
		return nil, fmt.Errorf("Failed to create Destination: %s", err)
	}
	return &Destination{
		connection: connection,
	}, nil
}

// NewDestination creates a destination from an existing connection. The connection
// must already be authenticated.
func NewDestinationFrom(connection *swift.Connection) *Destination {
	if connection.AuthToken == "" {
		panic(fmt.Errorf("NewDestinationFrom() was passed an unauthenticated connection"))
	}
	return &Destination{
		connection: connection,
	}
}

// AuthURL returns the authentication URL from this connection for use in HTTP requests.
func (d *Destination) AuthURL() string {
	return d.connection.StorageUrl
}

// AuthToken returns the x-auth-token from this connection for use in HTTP requests.
func (d *Destination) AuthToken() string {
	return d.connection.AuthToken
}
