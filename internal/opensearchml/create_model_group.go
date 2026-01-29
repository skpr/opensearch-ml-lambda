package opensearchml

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type CreateModelGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type CreateModelGroupResponse struct {
	ModelGroupID string `json:"model_group_id"`
}

func (c *Client) CreateModelGroup(ctx context.Context, req CreateModelGroupRequest) (CreateModelGroupResponse, error) {
	if id, found, err := c.FindModelGroupByName(ctx, req.Name); err != nil {
		return CreateModelGroupResponse{}, err
	} else if found {
		return CreateModelGroupResponse{
			ModelGroupID: id,
		}, nil
	}

	requestBodyBytes, err := json.Marshal(req)
	if err != nil {
		return CreateModelGroupResponse{}, fmt.Errorf("marshal model group create request: %w", err)
	}

	registerRequest, err := http.NewRequestWithContext(
		ctx,
		"POST",
		"/_plugins/_ml/model_groups/_register",
		bytes.NewReader(requestBodyBytes),
	)
	if err != nil {
		return CreateModelGroupResponse{}, fmt.Errorf("new request: %w", err)
	}

	registerRequest.Header.Set("Content-Type", "application/json")
	registerRequest.Header.Set("Accept", "application/json")

	resp, err := c.opensearch.Client.Perform(registerRequest)
	if err != nil {
		return CreateModelGroupResponse{}, fmt.Errorf("perform model group create request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("error closing response body: %v\n", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return CreateModelGroupResponse{}, fmt.Errorf("read model group create response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CreateModelGroupResponse{}, fmt.Errorf(
			"OpenSearch returned %d: %s",
			resp.StatusCode,
			string(body),
		)
	}

	var response CreateModelGroupResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return CreateModelGroupResponse{}, fmt.Errorf("parse model group create response: %w", err)
	}

	return response, nil
}
