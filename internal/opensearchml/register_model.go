package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type RegisterModelRequest struct {
	Name         string                          `json:"name"`
	FunctionName string                          `json:"function_name"`
	Description  string                          `json:"description"`
	ModelGroupID string                          `json:"model_group_id"`
	ConnectorID  string                          `json:"connector_id"`
	ModelFormat  string                          `json:"model_format"`
	ModelConfig  RegisterModelRequestModelConfig `json:"model_config"`
}

type RegisterModelRequestModelConfig struct {
	FrameworkType      string                               `json:"framework_type"`
	ModelType          string                               `json:"model_type"`
	EmbeddingDimension int                                  `json:"embedding_dimension"`
	AdditionalConfig   RegisterModelRequestAdditionalConfig `json:"additional_config"`
}

type RegisterModelRequestAdditionalConfig struct {
	SpaceType string `json:"space_type"`
}

type RegisterModelResponse struct {
	ModelID string `json:"model_id"`
}

// RegisterModel registers a model and returns the model_id.
func (c *Client) RegisterModel(ctx context.Context, req RegisterModelRequest) (RegisterModelResponse, error) {
	if id, ok, err := c.FindModel(ctx, req.Name); err != nil {
		return RegisterModelResponse{}, fmt.Errorf("find model: %w", err)
	} else if ok {
		return RegisterModelResponse{ModelID: id}, nil
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return RegisterModelResponse{}, fmt.Errorf("marshal register model payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_plugins/_ml/models/_register?deploy=true", bytes.NewReader(bodyBytes))
	if err != nil {
		return RegisterModelResponse{}, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.opensearch.Client.Perform(httpReq)
	if err != nil {
		return RegisterModelResponse{}, fmt.Errorf("perform register request: %w", err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	respBytes, _ := io.ReadAll(httpResp.Body)

	// If another caller created it between our check and register, re-check and return.
	if httpResp.StatusCode == http.StatusConflict {
		if id, ok, err := c.FindModel(ctx, req.Name); err != nil {
			return RegisterModelResponse{}, fmt.Errorf("register conflict; re-find model: %w", err)
		} else if ok {
			return RegisterModelResponse{ModelID: id}, nil
		}
	}

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return RegisterModelResponse{}, fmt.Errorf("register model failed: status=%d body=%s", httpResp.StatusCode, string(respBytes))
	}

	var resp RegisterModelResponse

	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return RegisterModelResponse{}, fmt.Errorf("unmarshal register response: %w (body=%s)", err, string(respBytes))
	}

	if resp.ModelID == "" {
		return RegisterModelResponse{}, fmt.Errorf("register response missing model_id (body=%s)", string(respBytes))
	}

	return resp, nil
}
