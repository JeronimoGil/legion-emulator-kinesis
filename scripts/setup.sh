#!/bin/bash

echo "Configuring LocalStack for Kinesis..."

source .env

echo "Waiting for LocalStack to start..."
sleep 5

echo "Creating Kinesis stream: $STREAM_NAME"
aws kinesis create-stream \
  --stream-name $STREAM_NAME \
  --shard-count $SHARD_COUNT \
  --endpoint-url $LOCALSTACK_ENDPOINT \
  --region $AWS_REGION \
  --no-cli-pager

echo "Stream created successfully"

echo "Verifying stream..."
aws kinesis describe-stream \
  --stream-name $STREAM_NAME \
  --endpoint-url $LOCALSTACK_ENDPOINT \
  --region $AWS_REGION \
  --no-cli-pager

echo "Configuration completed!"

