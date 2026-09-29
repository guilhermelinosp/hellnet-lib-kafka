package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func franzOptions(o Options) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.Brokers...),
		kgo.DialTimeout(10 * time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordDeliveryTimeout(o.TimeoutProduce),
	}
	if !o.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if o.SecurityProtocol == "ssl" || o.SecurityProtocol == "sasl_ssl" {
		tlsCfg, err := buildTLS(o)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if o.SecurityProtocol == "sasl_plaintext" || o.SecurityProtocol == "sasl_ssl" {
		mechanism, err := buildSASL(o)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

func buildTLS(o Options) (*tls.Config, error) {
	// #nosec G402 -- SSLInsecureSkipVerify is an explicit operator opt-in option.
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.SSLInsecureSkipVerify}
	if o.SSLCA == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(o.SSLCA)
	if err != nil {
		return nil, fmt.Errorf("kafka: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("kafka: invalid CA in %s", o.SSLCA)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func buildSASL(o Options) (sasl.Mechanism, error) {
	user, pass := o.SASLUsername, o.SASLPassword
	switch strings.ToUpper(o.SASLMechanism) {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "", "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", o.SASLMechanism)
	}
}
