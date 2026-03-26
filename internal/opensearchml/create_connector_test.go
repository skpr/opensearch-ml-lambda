package opensearchml

import "testing"

func TestParseModelIDsFromError(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "single model ID",
			body: `{"error":{"root_cause":[{"type":"status_exception","reason":"1 models are still using this connector, please undeploy the models first: [2CBOJJ0B0pnJUS8WL4sW]"}],"type":"status_exception","reason":"1 models are still using this connector, please undeploy the models first: [2CBOJJ0B0pnJUS8WL4sW]"},"status":400}`,
			want: []string{"2CBOJJ0B0pnJUS8WL4sW"},
		},
		{
			name: "multiple model IDs",
			body: `{"error":{"reason":"2 models are still using this connector, please undeploy the models first: [abc123,def456]"},"status":400}`,
			want: []string{"abc123", "def456"},
		},
		{
			name: "unrelated error",
			body: `{"error":{"reason":"something else went wrong"},"status":400}`,
			want: nil,
		},
		{
			name: "empty body",
			body: "",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseModelIDsFromError(tt.body)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
