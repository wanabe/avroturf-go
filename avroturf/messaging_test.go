package avroturf_test

import (
	"testing"

	"github.com/wanabe/avroturf-go/avroturf"
)

func TestFoo(t *testing.T) {
	messaging := avroturf.NewMessaging(
		"com.example",
		"./",
		"http://example.com",
	)

	if messaging.NameSpace != "com.example" {
		t.Errorf("unexpected namespace: %s", messaging.NameSpace)
	}
	if messaging.Registry.Upstream.RegistryURL != "http://example.com" {
		t.Errorf("unexpected registry url: %s", messaging.Registry.Upstream.RegistryURL)
	}
	if messaging.SchemaStore.Path != "./" {
		t.Errorf("unexpected schema store path: %s", messaging.SchemaStore.Path)
	}
}
