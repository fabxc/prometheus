package discovery

// import (
// 	"io"
// 	"os"
// 	"reflect"
// 	"testing"

// 	pb "github.com/prometheus/prometheus/config/generated"
// )

// func TestFileCreate(t *testing.T) {
// 	// Make sure this file doesn't exist.
// 	os.Remove("fixtures/_new_file")

// 	cfg := &pb.FileDiscovery{
// 		File: []string{"fixtures/file1", "fixtures/_new_file"},
// 	}
// 	fd, err := NewFileDiscovery(cfg)
// 	if err != nil {
// 		t.Fatalf("error creating file discovery", err)
// 	}

// 	err = fd.Start()
// 	if err != nil {
// 		t.Fatalf("error starting file discovery", err)
// 	}
// 	defer fd.Stop()
// 	defer os.Remove("fixtures/_new_file")

// 	if _, err := os.Stat("fixtures/_new_file"); err != nil {
// 		t.Fatalf("expected 'fixutes/_new_file' to be created but got: %s", err)
// 	}

// 	// Check if we can parse multiple target groups from a file
// 	expected := []*pb.TargetGroup{
// 		{
// 			Target: []string{"http://127.0.0.1:80/metrics", "http://127.0.1.1:80/metrics"},
// 			Labels: nil,
// 		}, {
// 			Target: []string{"http://127.0.0.1:81/metrics", "http://127.0.1.1:81/metrics"},
// 			Labels: nil,
// 		},
// 	}
// 	if !reflect.DeepEqual(fd.TargetGroups(), expected) {
// 		t.Error("unexpected target groups")
// 	}

// 	file, err := os.Open("fixtures/file1")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer file.Close()

// 	newFile, err := os.OpenFile("fixtures/_new_file", os.O_WRONLY, 0644)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err = io.Copy(newFile, file); err != nil {
// 		t.Fatal(err)
// 	}
// 	newFile.Close()

// 	expected = append(expected, expected...)
// 	if !reflect.DeepEqual(fd.TargetGroups(), expected) {
// 		t.Error("unexpected target groups")
// 	}
// }
