Experimental: Managed OpenSearch ML Configuration
=================================================

A Lambda for sync machine learning configuration for Manged OpenSearch.

Inspired by the "[Integrate with Amazon Titan Text Embeddings model through Amazon Bedrock](https://opensearch-ml-cfn-template.s3.amazonaws.com/bedrock-endpoint-with-vpc-domain-select-model.yml)" integration.

## Configuration

```
STREAM_NAME=<unique identifier for logging>
OPENSEARCH_ADDRESS=<DNS of the OpenSearch instance>
ROLE_ARN=<ARN which has Bedrock access>
BEDROCK_REGION=<AWS region for Bedrock, e.g. ap-southeast-2>
CONVERSE_MODEL=<Bedrock Converse model ID, e.g. amazon.nova-lite-v1:0>
CONVERSE_SYSTEM_PROMPT=<optional, default: "you are a helpful assistant.">
LLM_MODEL_NAME=<optional, default: "Bedrock Converse model">
```
