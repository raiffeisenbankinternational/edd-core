#!/bin/bash

set -exo pipefail
set -e

### Let's deal with the changes before stepping further.

# Run changes.py with --shallow and --check flags
./changes.py --shallow --check

### end changes

### Deploy CloudFormation stack for S3 bucket

echo "Deploying CloudFormation stack for S3 bucket..."

# Get EC2 instance role ARN using IMDSv2
echo "Getting EC2 instance role ARN using IMDSv2..."

# Get IMDSv2 token (valid for 21600 seconds = 6 hours)
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
  --connect-timeout 5 \
  --max-time 10 2>/dev/null || echo "")

if [ -z "$TOKEN" ]; then
  echo "ERROR: Failed to get IMDSv2 token. Not running on EC2 or metadata service unavailable."
  exit 1
fi

# Get IAM role name
ROLE_NAME=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/ \
  --connect-timeout 5 \
  --max-time 10 2>/dev/null || echo "")

if [ -z "$ROLE_NAME" ]; then
  echo "ERROR: Failed to get IAM role name from instance metadata."
  exit 1
fi

echo "Detected IAM role: $ROLE_NAME"

# Get AWS Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$ACCOUNT_ID" ]; then
  echo "ERROR: Failed to get AWS Account ID."
  exit 1
fi

echo "AWS Account ID: $ACCOUNT_ID"

# Construct role ARN
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "Role ARN: $ROLE_ARN"

# Deploy CloudFormation stack
STACK_NAME="edd-core-test-resources"
CF_TEMPLATE="test/cf/resources.yaml"

echo "Deploying CloudFormation stack: $STACK_NAME"

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$CF_TEMPLATE" \
  --parameter-overrides "InstanceRoleArn=$ROLE_ARN" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

if [ $? -ne 0 ]; then
  echo "ERROR: CloudFormation deployment failed!"
  exit 1
fi

echo "Waiting for stack to be ready..."

aws cloudformation wait stack-create-complete \
  --stack-name "$STACK_NAME" 2>/dev/null ||
  aws cloudformation wait stack-update-complete \
    --stack-name "$STACK_NAME" 2>/dev/null || true

# Verify stack status
STACK_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].StackStatus' \
  --output text)

if [[ ! "$STACK_STATUS" =~ ^(CREATE_COMPLETE|UPDATE_COMPLETE)$ ]]; then
  echo "ERROR: Stack deployment failed with status: $STACK_STATUS"
  aws cloudformation describe-stack-events \
    --stack-name "$STACK_NAME" \
    --max-items 10 \
    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
    --output table
  exit 1
fi

echo "CloudFormation stack deployed successfully with status: $STACK_STATUS"

# Get bucket name from stack outputs
BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
  --output text)

echo "S3 Bucket created: $BUCKET_NAME"

### Deploy DynamoDB stack

echo "Deploying CloudFormation stack for DynamoDB tables..."

DB_STACK_NAME="edd-core-test-db"
DB_CF_TEMPLATE="test/cf/db.yml"

echo "Deploying CloudFormation stack: $DB_STACK_NAME"

aws cloudformation deploy \
  --stack-name "$DB_STACK_NAME" \
  --template-file "$DB_CF_TEMPLATE" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

if [ $? -ne 0 ]; then
  echo "ERROR: DynamoDB CloudFormation deployment failed!"
  exit 1
fi

echo "Waiting for DynamoDB stack to be ready..."

aws cloudformation wait stack-create-complete \
  --stack-name "$DB_STACK_NAME" 2>/dev/null ||
  aws cloudformation wait stack-update-complete \
    --stack-name "$DB_STACK_NAME" 2>/dev/null || true

# Verify stack status
DB_STACK_STATUS=$(aws cloudformation describe-stacks \
  --stack-name "$DB_STACK_NAME" \
  --query 'Stacks[0].StackStatus' \
  --output text)

if [[ ! "$DB_STACK_STATUS" =~ ^(CREATE_COMPLETE|UPDATE_COMPLETE)$ ]]; then
  echo "ERROR: DynamoDB stack deployment failed with status: $DB_STACK_STATUS"
  aws cloudformation describe-stack-events \
    --stack-name "$DB_STACK_NAME" \
    --max-items 10 \
    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
    --output table
  exit 1
fi

echo "DynamoDB CloudFormation stack deployed successfully with status: $DB_STACK_STATUS"

### end CloudFormation deployment

# Create .env file for Docker Compose
echo "Creating .env file for Docker Compose..."

cat > .env <<EOF
DOCKER_URL=${DOCKER_URL:-}
EOF

echo ".env file created with DOCKER_URL=${DOCKER_URL:-}"

docker container prune --force

mkdir -p modules

docker compose down --remove-orphans
docker compose up -d

host="https://admin:admin@127.0.0.1:9200"
response="null"
count=1
until [[ "$response" = "200" ]] || [[ $count -gt 15 ]]; do
  response=$(curl -k --write-out %{http_code} --output /dev/null "$host" || echo " Fail (I guess not yet up)")
  >&2 echo "Elastic Search is unavailable ($count) - sleeping:  ${response}"
  sleep 10
  ((count++))
done

docker ps
docker compose logs postgres

echo "Running Postgres migrations"

docker compose run root-migration
docker compose run service-migration
docker compose run migration-test-edd-core
docker compose run migration-test-dimension

echo "Continue"
