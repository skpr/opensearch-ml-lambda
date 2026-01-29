package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Internal type for parsing search response.
type searchConnectorsResp struct {
	Hits struct {
		Hits []struct {
			ID     string `json:"_id"`
			Source struct {
				Name string `json:"name"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (c *Client) FindConnectorIDByName(ctx context.Context, name string) (string, bool, error) {
	payload := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"name": name,
			},
		},
		"size": 10,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", false, fmt.Errorf("marshal search payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_plugins/_ml/connectors/_search", bytes.NewReader(b))
	if err != nil {
		return "", false, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.opensearch.Client.Perform(req)
	if err != nil {
		return "", false, fmt.Errorf("perform search request: %w", err)
	}

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("search failed: status=%d body=%s", resp.StatusCode, string(respBytes))
	}

	var sr searchConnectorsResp

	if err := json.Unmarshal(respBytes, &sr); err != nil {
		return "", false, fmt.Errorf("unmarshal search response: %w (body=%s)", err, string(respBytes))
	}

	for _, h := range sr.Hits.Hits {
		if h.Source.Name == name && h.ID != "" {
			return h.ID, true, nil
		}
	}

	return "", false, nil
}
