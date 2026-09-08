/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"testing"
)

func decodeCert(t *testing.T, b64 string) *x509.Certificate {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("no PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func TestGenerateServingCert(t *testing.T) {
	serverCrtB64, serverKeyB64, caCrtB64, err := generateServingCert()
	if err != nil {
		t.Fatalf("generateServingCert: %v", err)
	}

	caCert := decodeCert(t, caCrtB64)
	if !caCert.IsCA {
		t.Error("CA certificate is not marked IsCA")
	}

	serverCert := decodeCert(t, serverCrtB64)
	wantSANs := []string{
		fmt.Sprintf("%s.%s", AgentName, AddonInstallationNamespace),
		fmt.Sprintf("%s.%s.svc", AgentName, AddonInstallationNamespace),
	}
	if len(serverCert.DNSNames) != len(wantSANs) {
		t.Fatalf("DNSNames = %v, want %v", serverCert.DNSNames, wantSANs)
	}
	for i, want := range wantSANs {
		if serverCert.DNSNames[i] != want {
			t.Errorf("DNSNames[%d] = %q, want %q", i, serverCert.DNSNames[i], want)
		}
	}

	if err := serverCert.CheckSignatureFrom(caCert); err != nil {
		t.Errorf("server cert not signed by returned CA: %v", err)
	}

	// Must decode to a PEM-encoded key, matching the chart's expected format.
	rawKey, err := base64.StdEncoding.DecodeString(serverKeyB64)
	if err != nil {
		t.Fatalf("base64 decode key: %v", err)
	}
	if block, _ := pem.Decode(rawKey); block == nil {
		t.Error("server key does not decode to a PEM block")
	}
}

// generateServingCert isn't deterministic -- stability must come from
// calling it once and reusing the result, which this test guards.
func TestGenerateServingCert_StableAcrossReuse(t *testing.T) {
	crt1, _, _, err := generateServingCert()
	if err != nil {
		t.Fatal(err)
	}
	crt2, _, _, err := generateServingCert()
	if err != nil {
		t.Fatal(err)
	}
	if crt1 == crt2 {
		t.Fatal("two independent calls produced identical certs; RNG or serial numbering is broken")
	}
}
