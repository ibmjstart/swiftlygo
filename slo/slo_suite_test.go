package slo_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSlo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Slo Suite")
}
