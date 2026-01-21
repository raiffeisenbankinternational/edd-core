#!/bin/bash
# Setup environment variables for integration tests
# This mimics the CI environment setup from Dockerfile

set -e

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up integration test environment...${NC}"

# Validate required tools
echo "Validating required tools..."
for tool in aws jq docker; do
  if ! command -v $tool &> /dev/null; then
    echo -e "${RED}ERROR: $tool is not installed or not in PATH${NC}"
    exit 1
  fi
done
echo -e "${GREEN}✓ All required tools found${NC}"

# Get AWS credentials and region
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || echo 'eu-west-1')}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"

if [ -z "$AWS_DEFAULT_REGION" ]; then
  echo -e "${RED}ERROR: AWS region not configured. Set AWS_DEFAULT_REGION or configure AWS CLI${NC}"
  exit 1
fi

echo "Using AWS region: $AWS_DEFAULT_REGION"

# Get account ID with error handling
echo "Getting AWS account ID..."
ACCOUNT_ID_JSON=$(aws sts get-caller-identity 2>&1)
if [ $? -ne 0 ]; then
  echo -e "${RED}ERROR: Failed to get AWS credentials${NC}"
  echo "$ACCOUNT_ID_JSON"
  exit 1
fi

export TARGET_ACCOUNT_ID="$(echo $ACCOUNT_ID_JSON | jq -r '.Account')"
export AccountId="$TARGET_ACCOUNT_ID"

if [ -z "$TARGET_ACCOUNT_ID" ] || [ "$TARGET_ACCOUNT_ID" = "null" ]; then
  echo -e "${RED}ERROR: Could not determine AWS account ID${NC}"
  exit 1
fi

echo -e "${GREEN}✓ AWS Account ID: $TARGET_ACCOUNT_ID${NC}"

# Assume PipelineRole (if it exists, otherwise use current credentials)
if aws iam get-role --role-name PipelineRole >/dev/null 2>&1; then
    echo -e "${YELLOW}Assuming PipelineRole...${NC}"
    cred=$(aws sts assume-role \
        --role-arn arn:aws:iam::${TARGET_ACCOUNT_ID}:role/PipelineRole \
        --role-session-name "edd-core-local-test-${RANDOM}" \
        --endpoint https://sts.${AWS_DEFAULT_REGION}.amazonaws.com \
        --region ${AWS_DEFAULT_REGION} 2>&1)
    
    if [ $? -ne 0 ]; then
      echo -e "${RED}ERROR: Failed to assume PipelineRole${NC}"
      echo "$cred"
      exit 1
    fi
    
    export AWS_ACCESS_KEY_ID=$(echo $cred | jq -r '.Credentials.AccessKeyId')
    export AWS_SECRET_ACCESS_KEY=$(echo $cred | jq -r '.Credentials.SecretAccessKey')
    export AWS_SESSION_TOKEN=$(echo $cred | jq -r '.Credentials.SessionToken')
    echo -e "${GREEN}✓ Successfully assumed PipelineRole${NC}"
else
    echo -e "${YELLOW}PipelineRole not found, using current AWS credentials${NC}"
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id 2>/dev/null || echo '')}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key 2>/dev/null || echo '')}"
    export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
fi

# Database configuration (for local docker-compose services)
export DatabasePassword="${DatabasePassword:-no-secret}"
export DatabaseEndpoint="${DatabaseEndpoint:-127.0.0.1}"
export DatabasePort="${DatabasePort:-5433}"
export EnvironmentNameLower="${EnvironmentNameLower:-pipeline}"
export ApplicationName="${ApplicationName:-dynamodb-svc}"

# Elasticsearch/OpenSearch configuration
export IndexDomainScheme="${IndexDomainScheme:-http}"
export IndexDomainEndpoint="${IndexDomainEndpoint:-127.0.0.1:9200}"

# Validate Docker is running
echo "Checking Docker status..."
if ! docker info >/dev/null 2>&1; then
  echo -e "${RED}ERROR: Docker is not running. Please start Docker and try again.${NC}"
  exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# Check if PostgreSQL is accessible
echo "Checking PostgreSQL availability..."
if docker ps --filter "name=postgres" --filter "status=running" | grep -q postgres; then
  echo -e "${GREEN}✓ PostgreSQL container is running${NC}"
else
  echo -e "${YELLOW}WARNING: PostgreSQL container not found. Run './start-it-services.sh' first${NC}"
fi

# Check if OpenSearch is accessible
echo "Checking OpenSearch availability..."
if docker ps --filter "name=opensearch-node1" --filter "status=running" | grep -q opensearch; then
  echo -e "${GREEN}✓ OpenSearch container is running${NC}"
else
  echo -e "${YELLOW}WARNING: OpenSearch container not found. Run './start-it-services.sh' first${NC}"
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Environment configured successfully!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "Configuration:"
echo "  AWS_REGION: $AWS_REGION"
echo "  AccountId: $AccountId"
echo "  EnvironmentNameLower: $EnvironmentNameLower"
echo "  ApplicationName: $ApplicationName"
echo "  DatabaseEndpoint: $DatabaseEndpoint:$DatabasePort"
echo "  IndexDomainEndpoint: $IndexDomainScheme://$IndexDomainEndpoint"
echo ""
echo "To use these exports in your shell, run:"
echo -e "  ${YELLOW}source ./setup-it-env.sh${NC}"
echo ""
echo "Or run tests directly:"
echo -e "  ${YELLOW}./setup-it-env.sh && clojure -M:test:it${NC}"
echo ""
