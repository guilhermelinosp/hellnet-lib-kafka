package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func urlPathEscape(s string) string {
	return url.PathEscape(s)
}

func newTimeoutContext(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// registryClient is a minimal Confluent-compatible Schema Registry client
// targeting the ccompat v6 API. It works against Apicurio Registry (the Hellnet
// schema registry, see hellnet-lib-schema scripts) and Confluent Schema
// Registry alike.
type registryClient struct {
	base    string
	path    string
	http    *http.Client
	timeout time.Duration
}

func newRegistryClient(url, path string) *registryClient {
	return &registryClient{
		base:    strings.TrimSuffix(url, "/"),
		path:    strings.TrimSuffix(path, "/"),
		http:    &http.Client{},
		timeout: 10 * time.Second,
	}
}

type schemaResponse struct {
	Schema string `json:"schema"`
	ID     int    `json:"id"`
}

func (c *registryClient) get(path string, out any) error {
	ctx, cancel := newTimeoutContext(c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kafka: schema registry %s: HTTP %d: %s", path, resp.StatusCode, truncate(body, 200))
	}
	return json.Unmarshal(body, out)
}

// latestSchema returns the latest schema string and id for a subject. The id
// is already uint32 (Confluent wire-format width); non-negative ids are the
// only values a compliant registry returns.
func (c *registryClient) latestSchema(subject string) (string, uint32, error) {
	var sr schemaResponse
	if err := c.get(c.path+"/subjects/"+urlPathEscape(subject)+"/versions/latest", &sr); err != nil {
		return "", 0, err
	}
	if sr.ID < 0 {
		return "", 0, fmt.Errorf("kafka: schema registry %s: invalid schema id %d", subject, sr.ID)
	}
	id := uint32(sr.ID) //nolint:gosec // G115: guarded by sr.ID < 0 above.
	return sr.Schema, id, nil
}

// schemaByID returns the schema string for a global schema id.
func (c *registryClient) schemaByID(id int) (string, error) {
	var sr schemaResponse
	if err := c.get(fmt.Sprintf(c.path+"/schemas/ids/%d", id), &sr); err != nil {
		return "", err
	}
	return sr.Schema, nil
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
