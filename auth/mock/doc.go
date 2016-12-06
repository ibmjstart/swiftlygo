/*
Package mock provides fake object storage endpoints for testing

The structs defined here all implement the github.com/ibmjstart/swiftlygo/auth.Destination
interface and are therefore useful for testing any code that
uploads data via a destination. It includes an endpoint that does nothing,
an endpoint that stores uploaded data in memory, and an endpoint that always
generates errors.
*/
package mock
