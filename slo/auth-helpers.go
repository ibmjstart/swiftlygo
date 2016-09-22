package slo

import (
	"fmt"
	"github.com/ncw/swift"
	"regexp"
	"strconv"
)

// GetAuthVersion extracts the OpenStack auth version from the end of an authURL.
func GetAuthVersion(url string) (int, error) {
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
// object store.
func Authenticate(username, apiKey, authURL, domain, tenant string) (swift.Connection, error) {
	version, err := GetAuthVersion(authURL)
	if err != nil {
		return swift.Connection{}, err
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
		return connection, fmt.Errorf("Failed to authenticate with object storage: %s", err)
	}
	return connection, nil
}
