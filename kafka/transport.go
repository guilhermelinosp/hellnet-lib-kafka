package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// newDialer returns a kafka-go Dialer for the configured security protocol,
// or nil for plaintext (the default local-dev path).
func newDialer(o Options) *kafka.Dialer {
	if o.SecurityProtocol == "" || o.SecurityProtocol == "plaintext" {
		return nil
	}
	d := &kafka.Dialer{Timeout: 10 * time.Second}

	if o.SecurityProtocol == "ssl" || o.SecurityProtocol == "sasl_ssl" {
		tlsCfg, err := buildTLS(o)
		if err == nil {
			d.TLS = tlsCfg
		}
	}
	if o.SecurityProtocol == "sasl_plaintext" || o.SecurityProtocol == "sasl_ssl" {
		if m, err := buildSASL(o); err == nil {
			d.SASLMechanism = m
		}
	}
	return d
}

// buildTLS builds a tls.Config from HELLNET_KAFKA_SSL_* options.
func buildTLS(o Options) (*tls.Config, error) {
	//nolint:gosec // G402: SSLInsecureSkipVerify is an explicit operator opt-in option.
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.SSLInsecureSkipVerify,
	}
	if o.SSLCA != "" {
		pem, err := os.ReadFile(o.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("kafka: read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: invalid CA in %s", o.SSLCA)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// buildSASL maps HELLNET_KAFKA_SASL_* to a kafka-go mechanism.
func buildSASL(o Options) (sasl.Mechanism, error) {
	user, pass := o.SASLUsername, o.SASLPassword
	switch strings.ToUpper(o.SASLMechanism) {
	case "PLAIN":
		return plain.Mechanism{Username: user, Password: pass}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, user, pass)
	case "", "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, user, pass)
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", o.SASLMechanism)
	}
}
