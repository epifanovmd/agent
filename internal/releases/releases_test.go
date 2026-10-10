package releases

import "testing"

// Где взять сборку: имя файла — в каталоге сборок (и в локальной папке); https:// — как есть;
// http:// — только при http-каталоге.
func TestBuildURL(t *testing.T) {
	for _, tc := range []struct{ releases, file, want string }{
		{"https://api.example.com/r", "report-1.0.0-linux-amd64", "https://api.example.com/r/report-1.0.0-linux-amd64"},
		{"https://api.example.com/r", "https://github.com/example/agent/releases/download/v1.1.0/netprobe-1.1.0-linux-amd64", "https://github.com/example/agent/releases/download/v1.1.0/netprobe-1.1.0-linux-amd64"},
		{"http://127.0.0.1:8080/r", "http://127.0.0.1:9000/x", "http://127.0.0.1:9000/x"},
		{"https://api.example.com/r", "http://cdn.example.com/x", ""},
		{"https://api.example.com/r", "../x", ""},
		{"https://api.example.com/r", "ftp://cdn.example.com/x", ""},
		{"https://api.example.com/r", "", ""},
		{"/srv/release", "report-1.0.0-linux-amd64.tar.gz", "/srv/release/report-1.0.0-linux-amd64.tar.gz"},
	} {
		got, err := BuildURL(tc.releases, tc.file)
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Errorf("%s %s: %q %v", tc.releases, tc.file, got, err)
		}
	}
}
