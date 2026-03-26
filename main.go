package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	requestsigner "github.com/opensearch-project/opensearch-go/v4/signer/awsv2"
	"github.com/skpr/yolog"
	"go-simpler.org/env"

	"github.com/skpr/opensearch-ml-lambda/internal/opensearchml"
)

// Config holds the environment configuration
type Config struct {
	StreamName        string `env:"STREAM_NAME,required"`
	OpenSearchAddress string `env:"OPENSEARCH_ADDRESS,required"`
	RoleARN           string `env:"ROLE_ARN,required"`
}

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context) error {
	var config Config

	if err := env.Load(&config, nil); err != nil {
		return err
	}

	logger := yolog.NewLogger(config.StreamName)
	defer logger.Log(os.Stdout)

	osConfig := opensearch.Config{
		Addresses: []string{
			config.OpenSearchAddress,
		},
	}

	logger.SetAttr("opensearch_address", config.OpenSearchAddress)

	awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return logger.WrapError(err)
	}

	signer, err := requestsigner.NewSignerWithService(awsConfig, "es")
	if err != nil {
		return logger.WrapError(err)
	}

	osConfig.Signer = signer

	osClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: osConfig,
	})
	if err != nil {
		return logger.WrapError(err)
	}

	client := opensearchml.NewClient(osClient)

	group := opensearchml.CreateModelGroupRequest{
		Name:        "Amazon Bedrock Model Group",
		Description: "Model group for Amazon Bedrock models",
	}

	connector := opensearchml.CreateConnectorRequest{
		Name:        "Amazon Bedrock Connector: Embedding",
		Description: "The connector to Bedrock Titan embedding model",
		Version:     1,
		Protocol:    "aws_sigv4",
		Parameters: opensearchml.CreateConnectorRequestParameters{
			Region:         "ap-southeast-2",
			ServiceName:    "bedrock",
			Model:          "amazon.titan-embed-text-v2:0",
			Dimensions:     1024,
			Normalize:      true,
			EmbeddingTypes: []string{"float"},
		},
		ClientConfig: opensearchml.CreateConnectorRequestClientConfig{
			MaxConnection:      10,
			ConnectionTimeout:  60000,
			ReadTimeout:        60000,
			RetryBackoffPolicy: "exponential_full_jitter",
			MaxReryTimes:       5,
			RetryBackoffMillis: 1000,
		},
		Credential: opensearchml.CreateConnectorRequestCredential{
			RoleARN: config.RoleARN,
		},
		Actions: []opensearchml.CreateConnectorRequestAction{
			{
				ActionType: "predict",
				Method:     "POST",
				URL:        "https://bedrock-runtime.${parameters.region}.amazonaws.com/model/${parameters.model}/invoke",
				Headers: map[string]string{
					"content-type":         "application/json",
					"x-amz-content-sha256": "required",
				},
				RequestBody:         `{ "inputText": "${parameters.inputText}", "dimensions": ${parameters.dimensions}, "normalize": ${parameters.normalize}, "embeddingTypes": ${parameters.embeddingTypes} }`,
				PreProcessFunction:  "connector.pre_process.bedrock.embedding",
				PostProcessFunction: "connector.post_process.bedrock.embedding",
			},
		},
	}

	groupResp, err := client.CreateModelGroup(ctx, group)
	if err != nil {
		return logger.WrapError(err)
	}

	logger.SetAttr("model_group_id", groupResp.ModelGroupID)

	connectorResp, err := client.CreateOrUpdateConnector(ctx, connector)
	if err != nil {
		return logger.WrapError(err)
	}

	logger.SetAttr("connector_id", connectorResp.ConnectorID)
	logger.SetAttr("model_undeployed", connectorResp.ModelUndeployed)
	logger.SetAttr("connector_changes", connectorResp.Changes)

	model := opensearchml.RegisterModelRequest{
		Name:         "bedrock titan embedding model v2",
		FunctionName: "remote",
		Description:  "test embedding model",
		ModelGroupID: groupResp.ModelGroupID,
		ConnectorID:  connectorResp.ConnectorID,
		ModelFormat:  "TORCH_SCRIPT",
		ModelConfig: opensearchml.RegisterModelRequestModelConfig{
			FrameworkType:      "sentence_transformers",
			ModelType:          "TEXT_EMBEDDING",
			EmbeddingDimension: 1024,
			AdditionalConfig: opensearchml.RegisterModelRequestAdditionalConfig{
				SpaceType: "l2",
			},
		},
	}

	modelResp, err := client.RegisterModel(ctx, model)
	if err != nil {
		return logger.WrapError(err)
	}

	logger.SetAttr("model_id", modelResp.ModelID)

	return nil
}
