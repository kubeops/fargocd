/*
Copyright AppsCode Inc. and Contributors.

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

package mode

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    Mode
		wantErr bool
	}{
		{"", InCluster, false},
		{"in-cluster", InCluster, false},
		{"autonomous", Autonomous, false},
		{"managed", Managed, false},
		{"Managed", "", true},
		{"hub", "", true},
	}
	for _, tc := range tests {
		got, err := Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) want error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("Parse(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRemotePrincipal(t *testing.T) {
	cases := map[Mode]bool{
		InCluster:  false,
		Autonomous: false,
		Managed:    true,
	}
	for m, want := range cases {
		if got := m.RemotePrincipal(); got != want {
			t.Errorf("%s.RemotePrincipal() = %t, want %t", m, got, want)
		}
	}
}
