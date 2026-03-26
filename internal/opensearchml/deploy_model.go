package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (c *Client) UndeployModel(ctx context.Context, modelID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("/_plugins/_ml/models/%s/_undeploy", modelID), nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	httpResp, err := c.opensearch.Client.Perform(httpReq)
	if err != nil {
		return fmt.Errorf("perform undeploy request: %w", err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("undeploy model failed: status=%d body=%s", httpResp.StatusCode, string(respBytes))
	}

	return nil
}

func (c *Client) DeployModel(ctx context.Context, modelID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("/_plugins/_ml/models/%s/_deploy", modelID), nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}

	httpResp, err := c.opensearch.Client.Perform(httpReq)
	if err != nil {
		return fmt.Errorf("perform deploy request: %w", err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("deploy model failed: status=%d body=%s", httpResp.StatusCode, string(respBytes))
	}

	return nil
}

func (c *Client) FindModelIDsByConnectorID(ctx context.Context, connectorID string) ([]string, error) {
	query := map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				"connector_id": connectorID,
			},
		},
		"size": 100,
	}

	bodyBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("marshal model search query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_plugins/_ml/models/_search", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("new model search request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.opensearch.Client.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("perform model search request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read model search response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("model search failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var searchResp searchModelResponse
	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return nil, fmt.Errorf("parse model search response: %w", err)
	}

	var ids []string
	for _, hit := range searchResp.Hits.Hits {
		if hit.ID != "" {
			ids = append(ids, hit.ID)
		}
	}

	return ids, nil
}
