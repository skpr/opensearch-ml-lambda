package opensearchml

import (
	"encoding/json"
	"testing"
)

func TestConnectorDiff(t *testing.T) {
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
				ActionType:  "PREDICT",
				Method:      "POST",
				URL:         "https://example.com/invoke",
				Headers:     map[string]string{"content-type": "application/json"},
				RequestBody: `{"inputText": "${parameters.inputText}"}`,
			},
		},
	}

	tests := []struct {
		name          string
		existing      string
		wantNoChanges bool
		wantPaths     []string
	}{
		{
			name:          "exact match",
			existing:      mustMarshal(t, req),
			wantNoChanges: true,
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
			wantNoChanges: true,
		},
		{
			name: "different description",
			existing: func() string {
				modified := req
				modified.Description = "Different description"
				return mustMarshal(t, modified)
			}(),
			wantPaths: []string{"description"},
		},
		{
			name: "different parameter",
			existing: func() string {
				modified := req
				modified.Parameters.Region = "us-east-1"
				return mustMarshal(t, modified)
			}(),
			wantPaths: []string{"parameters.region"},
		},
		{
			name: "different action count",
			existing: func() string {
				modified := req
				modified.Actions = nil
				return mustMarshal(t, modified)
			}(),
			wantPaths: []string{"actions"},
		},
		{
			name: "different credential is ignored",
			existing: func() string {
				modified := req
				modified.Credential.RoleARN = "arn:aws:iam::999999999999:role/other"
				return mustMarshal(t, modified)
			}(),
			wantNoChanges: true,
		},
		{
			name: "missing credential in existing is ignored",
			existing: func() string {
				m := mustUnmarshalMap(t, mustMarshal(t, req))
				delete(m, "credential")
				b, _ := json.Marshal(m)
				return string(b)
			}(),
			wantNoChanges: true,
		},
		{
			name: "JSON-encoded string parameter matches slice",
			existing: func() string {
				m := mustUnmarshalMap(t, mustMarshal(t, req))
				params := m["parameters"].(map[string]any)
				params["embeddingTypes"] = `["float"]`
				b, _ := json.Marshal(m)
				return string(b)
			}(),
			wantNoChanges: true,
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
			wantNoChanges: true,
		},
		{
			name: "multiple differences",
			existing: func() string {
				modified := req
				modified.Description = "Different"
				modified.Parameters.Region = "us-west-2"
				modified.Credential.RoleARN = "arn:aws:iam::000000000000:role/other"
				return mustMarshal(t, modified)
			}(),
			wantPaths: []string{"description", "parameters.region"},
		},
		{
			name: "missing field in existing",
			existing: func() string {
				m := mustUnmarshalMap(t, mustMarshal(t, req))
				delete(m, "protocol")
				b, _ := json.Marshal(m)
				return string(b)
			}(),
			wantPaths: []string{"protocol"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := connectorDiff(json.RawMessage(tt.existing), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantNoChanges {
				if len(changes) != 0 {
					t.Errorf("expected no changes, got %d: %+v", len(changes), changes)
				}
				return
			}

			gotPaths := make(map[string]bool)
			for _, c := range changes {
				gotPaths[c.Path] = true
			}

			for _, wantPath := range tt.wantPaths {
				if !gotPaths[wantPath] {
					t.Errorf("expected change at path %q, got changes: %+v", wantPath, changes)
				}
			}

			if len(changes) != len(tt.wantPaths) {
				t.Errorf("expected %d changes, got %d: %+v", len(tt.wantPaths), len(changes), changes)
			}
		})
	}
}

func TestConnectorDiffValues(t *testing.T) {
	req := CreateConnectorRequest{
		Name:        "test",
		Description: "new description",
		Version:     2,
		Protocol:    "aws_sigv4",
	}

	existing := CreateConnectorRequest{
		Name:        "test",
		Description: "old description",
		Version:     1,
		Protocol:    "aws_sigv4",
	}

	existingBytes := json.RawMessage(mustMarshal(t, existing))

	changes, err := connectorDiff(existingBytes, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	changeMap := make(map[string]ConnectorChange)
	for _, c := range changes {
		changeMap[c.Path] = c
	}

	if c, ok := changeMap["description"]; !ok {
		t.Fatal("expected description change")
	} else {
		if c.Desired != "new description" {
			t.Errorf("expected desired 'new description', got %v", c.Desired)
		}
		if c.Existing != "old description" {
			t.Errorf("expected existing 'old description', got %v", c.Existing)
		}
	}

	if c, ok := changeMap["version"]; !ok {
		t.Fatal("expected version change")
	} else {
		if c.Desired != float64(2) {
			t.Errorf("expected desired 2, got %v", c.Desired)
		}
		if c.Existing != float64(1) {
			t.Errorf("expected existing 1, got %v", c.Existing)
		}
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
