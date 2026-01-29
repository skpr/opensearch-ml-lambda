Experimental: Managed OpenSearch ML Configuration
=================================================

A Lambda for sync machine learning configuration for Manged OpenSearch.

Inspired by the "[Integrate with Amazon Titan Text Embeddings model through Amazon Bedrock](https://opensearch-ml-cfn-template.s3.amazonaws.com/bedrock-endpoint-with-vpc-domain-select-model.yml)" integration.

## Configuration

```
STREAM_NAME=<unique identifier for logging>
OPENSEARCH_ADDRESS=<DNS of the OpenSearch instance>
ROLE_ARN=<ARN which has Bedrock access>
```
