package kafka

import (
	"testing"
)

func TestBuildSerializerJSON(t *testing.T) {
	o := Default()
	o.DefaultSerializer = "json"
	s, err := o.buildSerializer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(JSONSerializer); !ok {
		t.Fatalf("expected JSONSerializer, got %T", s)
	}
}

func TestBuildSerializerAvro(t *testing.T) {
	o := Default()
	o.DefaultSerializer = "avro"
	if _, err := o.buildSerializer(); err == nil {
		t.Fatal("expected error without SchemaRegistryURL")
	}
	o.SchemaRegistryURL = "http://localhost:8085"
	s, err := o.buildSerializer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*AvroSerializer); !ok {
		t.Fatalf("expected *AvroSerializer, got %T", s)
	}
}

func TestLoadFromEnvSelectsAvro(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_DEFAULT_SERIALIZER", "avro")
	t.Setenv("HELLNET_KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8085")
	o := LoadFromEnv()
	s, err := o.ensureSerializer()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*AvroSerializer); !ok {
		t.Fatalf("expected *AvroSerializer, got %T", s)
	}
}