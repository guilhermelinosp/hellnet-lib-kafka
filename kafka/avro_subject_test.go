package kafka

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAvroSerializerStripsSubjectPrefix(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":7,"schema":"{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`))
	}))
	defer server.Close()

	o := testDefaultOptions()
	o.DefaultSerializer = "avro"
	o.SchemaRegistryURL = server.URL
	o.SchemaRegistryPath = ""
	o.SchemaSubjectStripPrefix = "br.com.hellnet."
	s, err := o.buildSerializer()
	if err != nil {
		t.Fatal(err)
	}
	type payload struct {
		ID string `avro:"id"`
	}
	data, err := s.(*AvroSerializer).SerializeContext(context.Background(), "br.com.hellnet.fast.order.accepted.v1", payload{ID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "/subjects/fast.order.accepted.v1/versions/latest"; path != want {
		t.Fatalf("registry path = %q, want %q", path, want)
	}
	if data[0] != 0 || data[4] != 7 {
		t.Fatalf("unexpected wire header % x", data[:5])
	}
	if !strings.HasPrefix(path, "/subjects/fast.") {
		t.Fatal("prefix was not stripped")
	}
}

func TestAvroSerializerKeepsTopicAsSubjectWithoutPrefix(t *testing.T) {
	a := &AvroSerializer{}
	if got := a.subject("orders"); got != "orders" {
		t.Fatalf("subject = %q", got)
	}
}
