package opensearchml

import (
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// Client for interacting with OpenSearch ML APIs
type Client struct {
	opensearch *opensearchapi.Client
}

// NewClient creates a new OpenSearch ML client
func NewClient(o *opensearchapi.Client) *Client {
	return &Client{
		opensearch: o,
	}
}
