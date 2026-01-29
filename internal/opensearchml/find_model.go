package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type searchModelResponse struct {
	Hits struct {
		Hits []struct {
			ID     string `json:"_id"`
			Source struct {
				Name string `json:"name"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (c *Client) FindModel(ctx context.Context, name string) (string, bool, error) {
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []map[string]any{
					{
						"term": map[string]any{
							"name.keyword": name,
						},
					},
				},
			},
		},
		"size": 1,
	}

	bodyBytes, err := json.Marshal(query)
	if err != nil {
		return "", false, fmt.Errorf("marshal model search query: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"/_plugins/_ml/models/_search",
		bytes.NewReader(bodyBytes),
	)
	if err != nil {
		return "", false, fmt.Errorf("new model search request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.opensearch.Client.Perform(req)
	if err != nil {
		return "", false, fmt.Errorf("perform model search request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("read model search response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf(
			"OpenSearch returned %d during model search: %s",
			resp.StatusCode,
			string(respBody),
		)
	}

	var searchResp searchModelResponse

	if err := json.Unmarshal(respBody, &searchResp); err != nil {
		return "", false, fmt.Errorf("parse model search response: %w", err)
	}

	if len(searchResp.Hits.Hits) == 0 {
		return "", false, nil
	}

	return searchResp.Hits.Hits[0].ID, true, nil
}
