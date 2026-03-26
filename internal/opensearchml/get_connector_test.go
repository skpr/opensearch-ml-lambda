package opensearchml

import (
	"encoding/json"
	"testing"
)

func TestConnectorMatchesRequest(t *testing.T) {
	req := CreateConnectorRequest{
		Name:        "test-connector",
		Description: "A test connector",
		Version:     1,
		Protocol:    "aws_sigv4",
		Parameters: CreateConnectorRequestParameters{
			Region:         "ap-southeast-2",
			ServiceName:    "bedrock",
			Model:          "amazon.titan-embed-text-v2:0",
			Dimensions:     1024,
			Normalize:      true,
			EmbeddingTypes: []string{"float"},
		},
		Credential: CreateConnectorRequestCredential{
			RoleARN: "arn:aws:iam::123456789012:role/test",
		},
		Actions: []CreateConnectorRequestAction{
			{
				ActionType: "predict",
				Method:     "POST",
				URL:        "https://example.com/invoke",
				Headers:    map[string]string{"content-type": "application/json"},
				RequestBody: `{"inputText": "${parameters.inputText}"}`,
			},
		},
	}

	tests := []struct {
		name     string
		existing string
		want     bool
	}{
		{
			name: "exact match",
			existing: mustMarshal(t, req),
			want: true,
		},
		{
			name: "match with extra API fields",
			existing: func() string {
				m := mustUnmarshalMap(t, mustMarshal(t, req))
				m["created_time"] = 1700000000000
				m["last_updated_time"] = 1700000000000
				m["owner"] = map[string]any{"name": "admin"}
				m["connector_id"] = "abc123"
				b, _ := json.Marshal(m)
				return string(b)
			}(),
			want: true,
		},
		{
			name: "different description",
			existing: func() string {
				modified := req
				modified.Description = "Different description"
				return mustMarshal(t, modified)
			}(),
			want: false,
		},
		{
			name: "different parameter",
			existing: func() string {
				modified := req
				modified.Parameters.Region = "us-east-1"
				return mustMarshal(t, modified)
			}(),
			want: false,
		},
		{
			name: "different action count",
			existing: func() string {
				modified := req
				modified.Actions = nil
				return mustMarshal(t, modified)
			}(),
			want: false,
		},
		{
			name: "different credential",
			existing: func() string {
				modified := req
				modified.Credential.RoleARN = "arn:aws:iam::999999999999:role/other"
				return mustMarshal(t, modified)
			}(),
			want: false,
		},
		{
			name: "extra nested API fields in parameters",
			existing: func() string {
				m := mustUnmarshalMap(t, mustMarshal(t, req))
				params := m["parameters"].(map[string]any)
				params["extra_internal_field"] = "should-be-ignored"
				b, _ := json.Marshal(m)
				return string(b)
			}(),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := connectorMatchesRequest(json.RawMessage(tt.existing), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("connectorMatchesRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustUnmarshalMap(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}
