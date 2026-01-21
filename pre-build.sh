#!/bin/bash

set -eo pipefail

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting edd-core pre-build setup...${NC}"

### Let's deal with the changes before stepping further.

# Run changes.py with --shallow and --check flags
echo "Checking for uncommitted changes..."
./changes.py --shallow --check

### end changes

### Deploy CloudFormation stack for S3 bucket

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Deploying CloudFormation stack for S3 bucket...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Get EC2 instance role ARN using IMDSv2
echo "Attempting to get EC2 instance role ARN using IMDSv2..."

# Get IMDSv2 token (valid for 21600 seconds = 6 hours)
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
  --connect-timeout 5 \
  --max-time 10 2>/dev/null || echo "")

if [ -z "$TOKEN" ]; then
  echo -e "${YELLOW}WARNING: Failed to get IMDSv2 token. Not running on EC2 or metadata service unavailable.${NC}"
  echo -e "${YELLOW}If running locally, this is expected. Continuing...${NC}"
  ROLE_ARN=""
else
  # Get IAM role name
  ROLE_NAME=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/iam/security-credentials/ \
    --connect-timeout 5 \
    --max-time 10 2>/dev/null || echo "")

  if [ -z "$ROLE_NAME" ]; then
    echo -e "${YELLOW}WARNING: Failed to get IAM role name from instance metadata.${NC}"
    ROLE_ARN=""
  else
    echo -e "${GREEN}✓ Detected IAM role: $ROLE_NAME${NC}"

    # Get AWS Account ID
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

    if [ -z "$ACCOUNT_ID" ]; then
      echo -e "${RED}ERROR: Failed to get AWS Account ID.${NC}"
      exit 1
    fi

    echo -e "${GREEN}✓ AWS Account ID: $ACCOUNT_ID${NC}"

    # Construct role ARN
    ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
    echo -e "${GREEN}✓ Role ARN: $ROLE_ARN${NC}"
  fi
fi

# Deploy CloudFormation stack
STACK_NAME="edd-core-test-resources"
CF_TEMPLATE="test/cf/resources.yaml"

echo "Deploying CloudFormation stack: $STACK_NAME"

# Build parameter overrides
# If we don't have a role ARN, use current AWS principal ARN as fallback
if [ -z "$ROLE_ARN" ]; then
  echo -e "${YELLOW}No EC2 instance role detected, using current AWS principal...${NC}"
  CALLER_ARN=$(aws sts get-caller-identity --query Arn --output text)
  ROLE_ARN="$CALLER_ARN"
  echo -e "${GREEN}✓ Using ARN: $ROLE_ARN${NC}"
fi

PARAM_OVERRIDES="InstanceRoleArn=$ROLE_ARN"

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$CF_TEMPLATE" \
  --parameter-overrides $PARAM_OVERRIDES \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

if [ $? -ne 0 ]; then
  echo -e "${RED}ERROR: CloudFormation deployment failed!${NC}"
  exit 1
fi

echo "Waiting for stack to be ready..."

# Wait for stack to complete (create or update)
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
  echo -e "${RED}ERROR: Stack deployment failed with status: $STACK_STATUS${NC}"
  echo -e "${YELLOW}Recent failed events:${NC}"
  aws cloudformation describe-stack-events \
    --stack-name "$STACK_NAME" \
    --max-items 10 \
    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
    --output table
  exit 1
fi

echo -e "${GREEN}✓ CloudFormation stack deployed successfully with status: $STACK_STATUS${NC}"

# Get bucket name from stack outputs
BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
  --output text)

echo -e "${GREEN}✓ S3 Bucket: $BUCKET_NAME${NC}"

### Deploy DynamoDB stack

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Deploying CloudFormation stack for DynamoDB tables...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

DB_STACK_NAME="edd-core-test-db"
DB_CF_TEMPLATE="dynamodb/files/db.yaml"

echo "Deploying CloudFormation stack: $DB_STACK_NAME"

aws cloudformation deploy \
  --stack-name "$DB_STACK_NAME" \
  --template-file "$DB_CF_TEMPLATE" \
  --parameter-overrides \
  "EnvironmentNameLower=pipeline" \
  "ApplicationName=dynamodb-svc" \
  "Realm=test" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

if [ $? -ne 0 ]; then
  echo -e "${RED}ERROR: DynamoDB CloudFormation deployment failed!${NC}"
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
  echo -e "${RED}ERROR: DynamoDB stack deployment failed with status: $DB_STACK_STATUS${NC}"
  echo -e "${YELLOW}Recent failed events:${NC}"
  aws cloudformation describe-stack-events \
    --stack-name "$DB_STACK_NAME" \
    --max-items 10 \
    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
    --output table
  exit 1
fi

echo -e "${GREEN}✓ DynamoDB CloudFormation stack deployed successfully with status: $DB_STACK_STATUS${NC}"

### Purge SQS queues before tests

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Purging SQS queues to ensure clean test state...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Get environment name (default to dev01)
ENV_NAME="${EnvironmentNameLower:-dev01}"

# Get account ID if not already set
if [ -z "$ACCOUNT_ID" ]; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
fi

# List of queues to purge
QUEUES=(
  "${ACCOUNT_ID}-${ENV_NAME}-it"
  "${ACCOUNT_ID}-${ENV_NAME}-it.fifo"
)

for QUEUE_NAME in "${QUEUES[@]}"; do
  # Check if queue exists
  QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_NAME" --query 'QueueUrl' --output text 2>/dev/null || echo "")

  if [ -n "$QUEUE_URL" ]; then
    echo "Purging queue: $QUEUE_NAME"
    if aws sqs purge-queue --queue-url "$QUEUE_URL" 2>/dev/null; then
      echo -e "${GREEN}✓ Successfully purged queue${NC}"
    else
      echo -e "${YELLOW}  Note: Queue was already empty or recently purged (60s cooldown applies)${NC}"
    fi
  else
    echo -e "${YELLOW}Queue not found (will be created by tests if needed): $QUEUE_NAME${NC}"
  fi
done

echo -e "${GREEN}✓ SQS queue purge complete${NC}"

### end CloudFormation deployment

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Setting up Docker environment...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Create .env file for Docker Compose
echo "Creating .env file for Docker Compose..."

cat >.env <<EOF
DOCKER_URL=${DOCKER_URL:-}
EOF

echo -e "${GREEN}✓ .env file created${NC}"

echo "Cleaning up old Docker resources..."
# Stop and remove all existing compose services and volumes
docker compose down --volumes --remove-orphans

# Also clean up any anonymous volumes that might be left behind
echo "Removing any orphaned anonymous volumes..."
docker volume prune -f

mkdir -p modules

echo "Starting fresh Docker Compose services..."
docker compose up -d

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Waiting for OpenSearch to be ready...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

host="http://127.0.0.1:9200"
response="null"
count=1
until [[ "$response" = "200" ]] || [[ $count -gt 15 ]]; do
  response=$(curl -k --write-out %{http_code} --silent --output /dev/null "$host" 2>/dev/null || echo "000")
  if [[ "$response" = "200" ]]; then
    echo -e "${GREEN}✓ OpenSearch is ready!${NC}"
  else
    echo -e "${YELLOW}Waiting for OpenSearch ($count/15) - HTTP $response${NC}"
    sleep 10
    ((count++))
  fi
done

if [[ "$response" != "200" ]]; then
  echo -e "${RED}ERROR: OpenSearch failed to start within timeout${NC}"
  echo ""
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${YELLOW}Diagnostics: Checking Docker container status...${NC}"
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  docker ps -a
  echo ""
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${YELLOW}Container logs (last 50 lines):${NC}"
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

  # Get OpenSearch container name
  OS_CONTAINER=$(docker compose ps -q opensearch 2>/dev/null || echo "")

  if [ -n "$OS_CONTAINER" ]; then
    echo -e "${YELLOW}OpenSearch container logs:${NC}"
    docker logs --tail 50 "$OS_CONTAINER"
  else
    echo -e "${RED}OpenSearch container not found!${NC}"
    echo "Available containers:"
    docker compose ps
  fi

  echo ""
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${YELLOW}All container logs:${NC}"
  echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  docker compose logs --tail 20

  exit 1
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Running Postgres migrations...${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo "Checking Docker container status..."
docker ps

echo ""
echo "Running migrations:"

echo "  1/4 root-migration..."
docker compose run --rm root-migration
echo -e "${GREEN}  ✓ root-migration complete${NC}"

echo "  2/4 service-migration..."
docker compose run --rm service-migration
echo -e "${GREEN}  ✓ service-migration complete${NC}"

echo "  3/4 migration-test-edd-core..."
docker compose run --rm migration-test-edd-core
echo -e "${GREEN}  ✓ migration-test-edd-core complete${NC}"

echo "  4/4 migration-test-dimension..."
docker compose run --rm migration-test-dimension
echo -e "${GREEN}  ✓ migration-test-dimension complete${NC}"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✓ Pre-build setup complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Environment is ready for integration tests."
echo "Run tests with:"
echo -e "  ${YELLOW}source ./setup-it-env.sh && clojure -M:test:it${NC}"
echo ""
