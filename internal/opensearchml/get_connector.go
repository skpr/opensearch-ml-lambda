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

func connectorDiff(existingRaw json.RawMessage, req CreateConnectorRequest) ([]ConnectorChange, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	var reqMap map[string]any
	if err := json.Unmarshal(reqBytes, &reqMap); err != nil {
		return nil, fmt.Errorf("unmarshal request to map: %w", err)
	}

	var existingMap map[string]any
	if err := json.Unmarshal(existingRaw, &existingMap); err != nil {
		return nil, fmt.Errorf("unmarshal existing to map: %w", err)
	}

	// Credentials are never returned by the API, so exclude from diff.
	delete(reqMap, "credential")
	delete(existingMap, "credential")

	var changes []ConnectorChange
	jsonDiff("", reqMap, existingMap, &changes)
	return changes, nil
}

// ConnectorChange describes a single field difference between the desired and existing connector.
type ConnectorChange struct {
	Path     string `json:"path"`
	Desired  any    `json:"desired"`
	Existing any    `json:"existing"`
}

func jsonDiff(prefix string, desired, actual any, changes *[]ConnectorChange) {
	switch d := desired.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			*changes = append(*changes, ConnectorChange{Path: prefix, Desired: desired, Existing: actual})
			return
		}
		for k, dv := range d {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			av, exists := a[k]
			if !exists {
				*changes = append(*changes, ConnectorChange{Path: p, Desired: dv, Existing: nil})
				continue
			}
			jsonDiff(p, dv, av, changes)
		}
	case []any:
		a, ok := actual.([]any)
		if !ok {
			if aStr, isStr := actual.(string); isStr {
				var decoded any
				if json.Unmarshal([]byte(aStr), &decoded) == nil {
					jsonDiff(prefix, desired, decoded, changes)
					return
				}
			}
			*changes = append(*changes, ConnectorChange{Path: prefix, Desired: desired, Existing: actual})
			return
		}
		if len(d) != len(a) {
			*changes = append(*changes, ConnectorChange{Path: prefix, Desired: desired, Existing: actual})
			return
		}
		for i := range d {
			jsonDiff(fmt.Sprintf("%s[%d]", prefix, i), d[i], a[i], changes)
		}
	default:
		if fmt.Sprintf("%v", desired) != fmt.Sprintf("%v", actual) {
			*changes = append(*changes, ConnectorChange{Path: prefix, Desired: desired, Existing: actual})
		}
	}
}
