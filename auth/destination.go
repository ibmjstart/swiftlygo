package auth

import (
	"bytes"
	"fmt"
	"github.com/ncw/swift"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// Destination defines a valid upload destination for files.
type Destination interface {
	CreateFile(container string, objectName string, checkHash bool, Hash string) (io.WriteCloser, error)
	CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error
	CreateDLO(manifestContainer, manifestName, objectContainer, filenamePrefix string) error
	FileNames(container string) ([]string, error)
	Objects(container string) ([]swift.Object, error)
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

// CreateSLO sends the provided json to the destination as an SLO manifest.
func (s *SwiftDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	targetUrl := s.SwiftConnection.StorageUrl + "/" + containerName + "/" + manifestName + "?multipart-manifest=put"

	request, err := http.NewRequest(http.MethodPut, targetUrl, bytes.NewReader(sloManifestJSON))
	if err != nil {
		return fmt.Errorf("Failed to create request for uploading manifest file: %s", err)
	}
	request.Header.Add("X-Auth-Token", s.SwiftConnection.AuthToken)
	request.Header.Add("Content-Length", strconv.Itoa(len(sloManifestJSON)))
	response, err := http.DefaultClient.Do(request)
	defer response.Body.Close()
	if err != nil {
		return fmt.Errorf("Error sending manifest upload request: %s", err)
	} else if response.StatusCode < 200 || response.StatusCode >= 300 {
		body := bytes.NewBufferString("")
		_, _ = body.ReadFrom(response.Body)
		return fmt.Errorf("Failed to upload manifest with status %d with reasons:\n%s\nand manifest:\n%s", response.StatusCode, body.String(), string(sloManifestJSON))
	}
	// Check the returned hash against our locally computed one. We need to strip the quotes off of the sides of the hash first
	if strings.Trim(response.Header["Etag"][0], "\"") != manifestEtag {
		return fmt.Errorf("Manifest corrupted on upload, please try again.")
	}
	return nil

}

// CreateDLO creates a dlo with the provided name and prefix in the given container.
func (s *SwiftDestination) CreateDLO(manifestContainer, manifestName, objectContainer, filenamePrefix string) error {
	manifest := objectContainer + "/" + filenamePrefix
	targetURL := s.SwiftConnection.StorageUrl + "/" + manifestContainer + "/" + manifestName

	request, err := http.NewRequest(http.MethodPut, targetURL, nil)
	if err != nil {
		return fmt.Errorf("Failed to create request for uploading manifest file: %s", err)
	}
	request.Header.Add("X-Auth-Token", s.SwiftConnection.AuthToken)
	request.Header.Add("X-Object-Manifest", manifest)

	response, err := http.DefaultClient.Do(request)
	defer response.Body.Close()
	if err != nil {
		return fmt.Errorf("Error sending manifest upload request: %s", err)
	} else if response.StatusCode < 200 || response.StatusCode >= 300 {
		body := bytes.NewBufferString("")
		_, _ = body.ReadFrom(response.Body)
		return fmt.Errorf("Failed to upload manifest with status %d with reasons:\n%s", response.StatusCode, body.String())
	}

	return nil
}

// FileNames returns a slice of the names of all files already in the destination container.
func (s *SwiftDestination) FileNames(container string) ([]string, error) {
	return s.SwiftConnection.ObjectNamesAll(container, nil)
}

// Objects returns a slice of swift Objects that container information about the container's
// contents.
func (s *SwiftDestination) Objects(container string) ([]swift.Object, error) {
	return s.SwiftConnection.ObjectsAll(container, nil)
}

// Ensure that SwiftDestination satsifies the interface at compile-time
var _ Destination = &SwiftDestination{}

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

// Authenticate logs in to OpenStack object storage and returns a connection to the
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

// AuthenticateWithToken logs in to OpenStack object storage using the authentication token and
// storage url and returns a connection to the object store. It also checks that the connection is
// valid.
func AuthenticateWithToken(authToken, storageUrl string) (Destination, error) {
	connection := swift.Connection{
		StorageUrl: storageUrl,
		AuthToken:  authToken,
	}

	if !connection.Authenticated() {
		return &SwiftDestination{SwiftConnection: &connection}, fmt.Errorf("Connection not authenticated")
	}

	return &SwiftDestination{SwiftConnection: &connection}, nil
}
