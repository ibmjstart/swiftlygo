/*
Package auth provides easy methods to authenticate with Object Storage

The main fixture of the auth package is the Destination interface.
Destination provides all of the features needed by the rest of
swiftlygo to manipulate object storage. The default implementation,
SwiftDestination, essentially wraps the github.com/ncw/swift.Connection.
We did this to make it easy to write tests against mock implementations
of the Destination interface. Those mock implementations can be found
in the mock subpackage.

The intended use of auth is to call either Authenticate() or
AuthenticateWithToken with your credentials to set up a Destination.

The names of the parameters to Authenticate may not match the names
of the credentials that your OpenStack Object Store provides. In
general, password and API Key are the same thing. Also domain may be
called domanName and tenant may be projectID. Domain and tenant are
optional parameters in some auth versions.

For IBM Bluemix object stores, you can follow the following pattern.
Assuming that your service credentials look as follow:

	{
	  "auth_url": "https://identity.open.softlayer.com",
	  "project": "project_string",
	  "projectId": "project_id",
	  "region": "dallas",
	  "userId": "user_id",
	  "username": "user_name",
	  "password": "password",
	  "domainId": "domain_id",
	  "domainName": "domain_name",
	  "role": "admin"
	}

You can authenticate with:

	Authenticate("user_name", "password", "https://identity.open.softlayer.com/v3", "domain_name", "")

Please note that we had to append "/v3" to the auth URL.

For Softlayer object stores, you can find your credentials in the user interface by clicking
"View Credentials" when viewing an Object Storage instance in the Web UI.

You should see something like:

	Authentication Endpoint:
	Public: https://public-url.com.../v1.0/
	Private: https://private-url.com.../v1.0/

	Username:
	username...

	API Key (Password):
	password...

You can authenticate with:

	Authenticate("username...", "password...", "https:public-url.com.../v1.0/", "", "")

*/
package auth
