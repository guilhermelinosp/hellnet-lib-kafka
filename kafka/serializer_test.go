package kafka

import (
	"context"
	"testing"
)

func TestBuildSerializerJSON(t *testing.T) {
	o := Default()
	o.DefaultSerializer = "json"
	s, err := o.buildSerializer(context.Background())
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
	if _, err := o.buildSerializer(context.Background()); err == nil {
		t.Fatal("expected error without SchemaRegistryURL")
	}
	o.SchemaRegistryURL = "http://localhost:8085"
	s, err := o.buildSerializer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*AvroSerializer); !ok {
		t.Fatalf("expected *AvroSerializer, got %T", s)
	}
}

func TestBuildSerializerProtobuf(t *testing.T) {
	o := Default()
	o.DefaultSerializer = "protobuf"
	if _, err := o.buildSerializer(context.Background()); err == nil {
		t.Fatal("expected error without SchemaRegistryURL")
	}
	o.SchemaRegistryURL = "http://localhost:8085"
	s, err := o.buildSerializer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*ProtobufSerializer); !ok {
		t.Fatalf("expected *ProtobufSerializer, got %T", s)
	}
}

func TestLoadFromEnvSelectsAvro(t *testing.T) {
	t.Setenv("HELLNET_KAFKA_DEFAULT_SERIALIZER", "avro")
	t.Setenv("HELLNET_KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8085")
	o := LoadFromEnv()
	s, err := o.ensureSerializer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*AvroSerializer); !ok {
		t.Fatalf("expected *AvroSerializer, got %T", s)
	}
}
