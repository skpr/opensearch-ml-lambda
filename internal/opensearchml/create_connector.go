package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// https://docs.aws.amazon.com/opensearch-service/latest/developerguide/ml-amazon-connector.html
type CreateConnectorRequest struct {
	Name         string                             `json:"name"`
	Description  string                             `json:"description"`
	Version      int                                `json:"version"`
	ClientConfig CreateConnectorRequestClientConfig `json:"client_config,omitempty"`
	Protocol     string                             `json:"protocol"`
	Parameters   CreateConnectorRequestParameters   `json:"parameters"`
	Credential   CreateConnectorRequestCredential   `json:"credential"`
	Actions      []CreateConnectorRequestAction     `json:"actions"`
}

type CreateConnectorRequestClientConfig struct {
	MaxConnection       int    `json:"max_connection,omitempty"`
	ConnectionTimeout   int    `json:"connection_timeout,omitempty"`
	ReadTimeout         int    `json:"read_timeout,omitempty"`
	RetryBackoffPolicy  string `json:"retry_backoff_policy,omitempty"`
	MaxReryTimes        int    `json:"max_retry_times,omitempty"`
	RetryBackoffMillis  int    `json:"retry_backoff_millis,omitempty"`
	RetryTimeoutSeconds int    `json:"retry_timeout_seconds,omitempty"`
	SkipSSLVerification bool   `json:"skip_ssl_verification,omitempty"`
}

type CreateConnectorRequestParameters struct {
	Region         string   `json:"region"`
	ServiceName    string   `json:"service_name"`
	Model          string   `json:"model"`
	Dimensions     int      `json:"dimensions"`
	Normalize      bool     `json:"normalize"`
	EmbeddingTypes []string `json:"embeddingTypes"`
}

type CreateConnectorRequestCredential struct {
	RoleARN   string `json:"roleArn,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
}

type CreateConnectorRequestAction struct {
	ActionType          string            `json:"action_type"`
	Method              string            `json:"method"`
	URL                 string            `json:"url"`
	Headers             map[string]string `json:"headers"`
	RequestBody         string            `json:"request_body"`
	PreProcessFunction  string            `json:"pre_process_function"`
	PostProcessFunction string            `json:"post_process_function"`
}

type CreateOrUpdateConnectorResponse struct {
	ConnectorID string `json:"connector_id"`
}

func (c *Client) CreateOrUpdateConnector(ctx context.Context, req CreateConnectorRequest) (CreateOrUpdateConnectorResponse, error) {
	if req.Name == "" {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("connector name is required")
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("marshal connector payload: %w", err)
	}

	// If a connector with this name already exists, update it via PUT.
	if id, ok, err := c.FindConnectorIDByName(ctx, req.Name); err != nil {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("find connector by name: %w", err)
	} else if ok {
		if err := c.updateConnector(ctx, id, bodyBytes); err != nil {
			return CreateOrUpdateConnectorResponse{}, fmt.Errorf("update connector: %w", err)
		}

		return CreateOrUpdateConnectorResponse{ConnectorID: id}, nil
	}

	// Connector does not exist yet, create it via POST.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_plugins/_ml/connectors/_create", bytes.NewReader(bodyBytes))
	if err != nil {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.opensearch.Client.Perform(httpReq)
	if err != nil {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("perform create request: %w", err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	respBytes, _ := io.ReadAll(httpResp.Body)

	// If another caller created it between our check and create, update the existing connector.
	if httpResp.StatusCode == http.StatusConflict {
		if id, ok, err := c.FindConnectorIDByName(ctx, req.Name); err != nil {
			return CreateOrUpdateConnectorResponse{}, fmt.Errorf("create conflict; re-find connector: %w", err)
		} else if ok {
			if err := c.updateConnector(ctx, id, bodyBytes); err != nil {
				return CreateOrUpdateConnectorResponse{}, fmt.Errorf("create conflict; update connector: %w", err)
			}

			return CreateOrUpdateConnectorResponse{ConnectorID: id}, nil
		}
	}

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("create connector failed: status=%d body=%s", httpResp.StatusCode, string(respBytes))
	}

	var out CreateOrUpdateConnectorResponse

	if err := json.Unmarshal(respBytes, &out); err != nil {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("unmarshal create response: %w (body=%s)", err, string(respBytes))
	}

	if out.ConnectorID == "" {
		return CreateOrUpdateConnectorResponse{}, fmt.Errorf("create response missing connector_id (body=%s)", string(respBytes))
	}

	return out, nil
}

// updateConnector sends a PUT request to update an existing connector.
func (c *Client) updateConnector(ctx context.Context, connectorID string, bodyBytes []byte) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("/_plugins/_ml/connectors/%s", connectorID), bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.opensearch.Client.Perform(httpReq)
	if err != nil {
		return fmt.Errorf("perform update request: %w", err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(httpResp.Body)
		return fmt.Errorf("update connector failed: status=%d body=%s", httpResp.StatusCode, string(respBytes))
	}

	return nil
}
