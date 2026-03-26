package opensearchml

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (c *Client) getConnectorRaw(ctx context.Context, connectorID string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("/_plugins/_ml/connectors/%s", connectorID), nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.opensearch.Client.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("perform get connector request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read get connector response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("get connector failed: status=%d body=%s", resp.StatusCode, string(respBytes))
	}

	return respBytes, nil
}

func connectorMatchesRequest(existingRaw json.RawMessage, req CreateConnectorRequest) (bool, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}

	var reqMap map[string]any
	if err := json.Unmarshal(reqBytes, &reqMap); err != nil {
		return false, fmt.Errorf("unmarshal request to map: %w", err)
	}

	var existingMap map[string]any
	if err := json.Unmarshal(existingRaw, &existingMap); err != nil {
		return false, fmt.Errorf("unmarshal existing to map: %w", err)
	}

	return jsonSubsetEqual(reqMap, existingMap), nil
}

func jsonSubsetEqual(desired, actual any) bool {
	switch d := desired.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for k, dv := range d {
			av, exists := a[k]
			if !exists {
				return false
			}
			if !jsonSubsetEqual(dv, av) {
				return false
			}
		}
		return true
	case []any:
		a, ok := actual.([]any)
		if !ok {
			return false
		}
		if len(d) != len(a) {
			return false
		}
		for i := range d {
			if !jsonSubsetEqual(d[i], a[i]) {
				return false
			}
		}
		return true
	default:
		return fmt.Sprintf("%v", desired) == fmt.Sprintf("%v", actual)
	}
}
