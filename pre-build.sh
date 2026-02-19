#!/bin/bash
# Pre-build setup for integration tests.
# This script is designed to be sourced: source ./pre-build.sh
# It exports all environment variables needed by integration tests.

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

docker rm -f $(docker ps -aq)

_prebuild_main() {
  set -eo pipefail

  echo -e "${GREEN}Starting edd-core pre-build setup...${NC}"

  ### Let's deal with the changes before stepping further.

  # Run changes.py with --shallow and --check flags
  echo "Checking for uncommitted changes..."
  ./changes.py --shallow --check

  ### end changes

  ### Setup AWS credentials and region

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Setting up AWS credentials...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  # Validate required tools
  for tool in aws jq docker; do
    if ! command -v $tool &>/dev/null; then
      echo -e "${RED}ERROR: $tool is not installed or not in PATH${NC}"
      return 1
    fi
  done
  echo -e "${GREEN}All required tools found${NC}"

  # Fetch region from EC2 instance metadata (IMDSv2)
  echo "Fetching AWS region from EC2 instance metadata (IMDSv2)..."
  IMDS_TOKEN=$(curl -s -f -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
    --connect-timeout 5 \
    --max-time 10 2>/dev/null || echo "")

  if [ -z "$IMDS_TOKEN" ]; then
    echo -e "${RED}ERROR: Failed to get IMDSv2 token. Not running on EC2 or metadata service unavailable.${NC}"
    return 1
  fi

  EC2_REGION=$(curl -s -f -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
    "http://169.254.169.254/latest/meta-data/placement/region" \
    --connect-timeout 5 \
    --max-time 10 2>/dev/null || echo "")

  if [ -z "$EC2_REGION" ]; then
    echo -e "${RED}ERROR: Failed to fetch region from EC2 instance metadata.${NC}"
    return 1
  fi

  export AWS_DEFAULT_REGION="$EC2_REGION"
  export AWS_REGION="$EC2_REGION"
  export Region="$EC2_REGION"

  echo -e "${GREEN}AWS region from EC2 metadata: $AWS_DEFAULT_REGION${NC}"

  # Get account ID
  echo "Getting AWS account ID..."
  ACCOUNT_ID_JSON=$(aws sts get-caller-identity 2>&1)
  if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to get AWS credentials${NC}"
    echo "$ACCOUNT_ID_JSON"
    return 1
  fi

  export TARGET_ACCOUNT_ID="$(echo $ACCOUNT_ID_JSON | jq -r '.Account')"
  export AccountId="$TARGET_ACCOUNT_ID"

  if [ -z "$TARGET_ACCOUNT_ID" ] || [ "$TARGET_ACCOUNT_ID" = "null" ]; then
    echo -e "${RED}ERROR: Could not determine AWS account ID${NC}"
    return 1
  fi

  echo -e "${GREEN}AWS Account ID: $TARGET_ACCOUNT_ID${NC}"

  # Assume PipelineRole (if it exists, otherwise use current credentials)
  if aws iam get-role --role-name PipelineRole >/dev/null 2>&1; then
    echo -e "${YELLOW}Assuming PipelineRole...${NC}"
    cred=$(aws sts assume-role \
      --role-arn arn:aws:iam::${TARGET_ACCOUNT_ID}:role/PipelineRole \
      --role-session-name "edd-core-pre-build-${RANDOM}" \
      --endpoint https://sts.${AWS_DEFAULT_REGION}.amazonaws.com \
      --region ${AWS_DEFAULT_REGION} 2>&1)

    if [ $? -ne 0 ]; then
      echo -e "${RED}ERROR: Failed to assume PipelineRole${NC}"
      echo "$cred"
      return 1
    fi

    export AWS_ACCESS_KEY_ID=$(echo $cred | jq -r '.Credentials.AccessKeyId')
    export AWS_SECRET_ACCESS_KEY=$(echo $cred | jq -r '.Credentials.SecretAccessKey')
    export AWS_SESSION_TOKEN=$(echo $cred | jq -r '.Credentials.SessionToken')
    echo -e "${GREEN}Successfully assumed PipelineRole${NC}"
  else
    echo -e "${YELLOW}PipelineRole not found, using current AWS credentials${NC}"
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$(aws configure get aws_access_key_id 2>/dev/null || echo '')}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$(aws configure get aws_secret_access_key 2>/dev/null || echo '')}"
    export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
  fi

  ### Setup test environment variables

  export EnvironmentNameLower=pipeline
  export ResourceName=it-dynamodb
  export PublicHostedZoneName="${PublicHostedZoneName:-example.com}"
  export DatabaseName="${ResourceName}"
  export DatabasePassword="${DatabasePassword:-no-secret}"
  export DatabaseEndpoint="${DatabaseEndpoint:-127.0.0.1}"
  export DatabasePort="${DatabasePort:-5433}"
  export IndexDomainScheme="${IndexDomainScheme:-http}"
  export IndexDomainEndpoint="${IndexDomainEndpoint:-127.0.0.1:9200}"

  ### Deploy CloudFormation stack for S3 bucket

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Deploying CloudFormation stack for S3 bucket...${NC}"
  echo -e "${GREEN}======================================================${NC}"

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
      echo -e "${GREEN}Detected IAM role: $ROLE_NAME${NC}"

      # Construct role ARN
      ROLE_ARN="arn:aws:iam::${TARGET_ACCOUNT_ID}:role/${ROLE_NAME}"
      echo -e "${GREEN}Role ARN: $ROLE_ARN${NC}"
    fi
  fi

  # If we don't have a role ARN, use current AWS principal ARN as fallback
  if [ -z "$ROLE_ARN" ]; then
    echo -e "${YELLOW}No EC2 instance role detected, using current AWS principal...${NC}"
    CALLER_ARN=$(aws sts get-caller-identity --query Arn --output text)
    ROLE_ARN="$CALLER_ARN"
    echo -e "${GREEN}Using ARN: $ROLE_ARN${NC}"
  fi

  # Deploy CloudFormation stack
  STACK_NAME="edd-core-test-resources"
  CF_TEMPLATE="test/cf/resources.yaml"

  echo "Deploying CloudFormation stack: $STACK_NAME"

  PARAM_OVERRIDES="InstanceRoleArn=$ROLE_ARN EnvironmentNameLower=$EnvironmentNameLower"

  aws cloudformation deploy \
    --stack-name "$STACK_NAME" \
    --template-file "$CF_TEMPLATE" \
    --parameter-overrides $PARAM_OVERRIDES \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset

  if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: CloudFormation deployment failed!${NC}"
    return 1
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
    return 1
  fi

  echo -e "${GREEN}CloudFormation stack deployed successfully with status: $STACK_STATUS${NC}"

  # Get bucket name from stack outputs
  BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
    --output text)

  echo -e "${GREEN}S3 Bucket: $BUCKET_NAME${NC}"

  ### Deploy DynamoDB stack

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Deploying CloudFormation stack for DynamoDB tables...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  # DynamoDB naming derived from test environment variables
  DB_ENV_UPPER="PIPELINE"
  DB_REALM="test"
  DB_STACK_NAME="${DB_ENV_UPPER}-DYNAMODB-${ResourceName}-stack"
  DB_CF_TEMPLATE="dynamodb/files/db.yaml"

  echo "  ResourceName: $ResourceName"
  echo "  DatabaseName: $DatabaseName"
  echo "  Realm: $DB_REALM"
  echo "Deploying CloudFormation stack: $DB_STACK_NAME"

  aws cloudformation deploy \
    --stack-name "$DB_STACK_NAME" \
    --template-file "$DB_CF_TEMPLATE" \
    --parameter-overrides \
    "EnvironmentNameLower=$EnvironmentNameLower" \
    "DatabaseName=$DatabaseName" \
    "Realm=$DB_REALM" \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset

  if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: DynamoDB CloudFormation deployment failed!${NC}"
    return 1
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
    return 1
  fi

  echo -e "${GREEN}DynamoDB CloudFormation stack deployed successfully with status: $DB_STACK_STATUS${NC}"

  ### Purge SQS queues before tests

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Purging SQS queues to ensure clean test state...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  # List of queues to purge
  QUEUES=(
    "${TARGET_ACCOUNT_ID}-${EnvironmentNameLower}-it"
    "${TARGET_ACCOUNT_ID}-${EnvironmentNameLower}-it.fifo"
  )

  for QUEUE_NAME in "${QUEUES[@]}"; do
    # Check if queue exists
    QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_NAME" --query 'QueueUrl' --output text 2>/dev/null || echo "")

    if [ -n "$QUEUE_URL" ]; then
      echo "Purging queue: $QUEUE_NAME"
      if aws sqs purge-queue --queue-url "$QUEUE_URL" 2>/dev/null; then
        echo -e "${GREEN}Successfully purged queue${NC}"
      else
        echo -e "${YELLOW}  Note: Queue was already empty or recently purged (60s cooldown applies)${NC}"
      fi
    else
      echo -e "${YELLOW}Queue not found (will be created by tests if needed): $QUEUE_NAME${NC}"
    fi
  done

  echo -e "${GREEN}SQS queue purge complete${NC}"

  ### Setup Docker environment

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Setting up Docker environment...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  # Create .env file for Docker Compose (only if DOCKER_URL is set)
  # Save DOCKER_URL for later use (e.g. flyway migrations) before unsetting
  _DOCKER_URL_PREFIX=""
  if [ -n "${DOCKER_URL:-}" ]; then
    # Ensure DOCKER_URL has trailing slash for Dockerfile compatibility
    # (Dockerfiles don't support conditional slash, so we include it in the variable)
    DOCKER_URL_WITH_SLASH="${DOCKER_URL%/}/"
    _DOCKER_URL_PREFIX="$DOCKER_URL_WITH_SLASH"
    echo "Creating .env file for Docker Compose with DOCKER_URL=$DOCKER_URL_WITH_SLASH..."
    cat >.env <<EOF
DOCKER_URL=${DOCKER_URL_WITH_SLASH}
EOF
    # Unset the environment variable so docker-compose uses .env file instead
    # (environment variables take precedence over .env file, which can cause issues)
    unset DOCKER_URL
    echo -e "${GREEN}.env file created (DOCKER_URL unset from environment to use .env file)${NC}"
  else
    # Remove .env file if it exists to avoid empty DOCKER_URL
    rm -f .env
    echo -e "${GREEN}No DOCKER_URL set, using default Docker registry${NC}"
  fi

  echo "Cleaning up old Docker resources..."
  # Stop and remove all existing compose services and volumes
  docker compose down --volumes --remove-orphans

  # Also clean up any anonymous volumes that might be left behind
  echo "Removing any orphaned anonymous volumes..."
  docker volume prune -f

  mkdir -p modules

  echo "Starting fresh Docker Compose services..."
  docker compose up -d

  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Waiting for OpenSearch to be ready...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  host="http://127.0.0.1:9200"
  response="null"
  count=1
  until [[ "$response" = "200" ]] || [[ $count -gt 15 ]]; do
    response=$(curl -k --write-out %{http_code} --silent --output /dev/null "$host" 2>/dev/null || echo "000")
    if [[ "$response" = "200" ]]; then
      echo -e "${GREEN}OpenSearch is ready!${NC}"
    else
      echo -e "${YELLOW}Waiting for OpenSearch ($count/15) - HTTP $response${NC}"
      sleep 10
      ((count++))
    fi
  done

  if [[ "$response" != "200" ]]; then
    echo -e "${RED}ERROR: OpenSearch failed to start within timeout${NC}"
    echo ""
    echo -e "${YELLOW}======================================================${NC}"
    echo -e "${YELLOW}Diagnostics: Checking Docker container status...${NC}"
    echo -e "${YELLOW}======================================================${NC}"
    docker ps -a
    echo ""
    echo -e "${YELLOW}======================================================${NC}"
    echo -e "${YELLOW}Container logs (last 50 lines):${NC}"
    echo -e "${YELLOW}======================================================${NC}"

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
    echo -e "${YELLOW}======================================================${NC}"
    echo -e "${YELLOW}All container logs:${NC}"
    echo -e "${YELLOW}======================================================${NC}"
    docker compose logs --tail 20

    return 1
  fi

  echo ""
  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Running Postgres migrations...${NC}"
  echo -e "${GREEN}======================================================${NC}"

  echo "Checking Docker container status..."
  docker ps

  # Helper function to run flyway migration via Docker
  run_flyway_migration() {
    local schema=$1
    local locations=$2
    local volumes=$3

    docker run --rm --network edd-core_opensearch-net \
      $volumes \
      ${_DOCKER_URL_PREFIX}flyway/flyway:10.8 \
      -url=jdbc:postgresql://postgres:5432/postgres \
      -schemas="$schema" \
      -user=postgres \
      -password="no-secret" \
      -connectRetries=60 \
      -skipCheckForUpdate \
      -placeholders.historyPartitionCount=1 \
      -locations="$locations" \
      migrate
  }

  echo ""
  echo "Running migrations:"

  # Loop 1: Root realm schemas (EDD core tables - event store, history, etc.)
  # These use sql/files/edd migrations
  TEST_REALMS=(
    "test"
    "prod"
    "realm_a"
    "realm_b"
    "realm_x"
    "realm_y"
    "realm_1"
    "realm_2"
  )

  ROOT_VOLUMES="-v $(pwd)/sql/files/edd:/flyway/sql"
  ROOT_LOCATIONS="filesystem:/flyway/sql"

  echo -e "${YELLOW}Migrating root realm schemas (EDD core tables)...${NC}"
  for schema in "${TEST_REALMS[@]}"; do
    echo "  Migrating '${schema}' schema..."
    run_flyway_migration "$schema" "$ROOT_LOCATIONS" "$ROOT_VOLUMES"
    echo -e "${GREEN}  ${schema} schema complete${NC}"
  done

  # Loop 2: Service schemas (view store tables - aggregates, history, etc.)
  # These use modules/edd-core-view-store-postgres/migrations + test migrations
  TEST_SCHEMAS=(
    "test_local_svc"
    "test_local_test"
    "test_test_service"
    "test_edd_core"
    "test_glms_dimension_svc"
    "test_glms_application_svc"
    "prod_test_service"
    "realm_a_test_service"
    "realm_b_test_service"
    "realm_x_test_service"
    "realm_y_test_service"
    "realm_1_test_service"
    "realm_2_test_service"
  )

  SVC_VOLUMES="-v $(pwd)/modules/edd-core-view-store-postgres/migrations:/flyway/sql/prod -v $(pwd)/modules/edd-core-view-store-postgres/test/resources/migrations:/flyway/sql/test"
  SVC_LOCATIONS="filesystem:/flyway/sql/prod,filesystem:/flyway/sql/test"

  echo -e "${YELLOW}Migrating service schemas (view store tables)...${NC}"
  for schema in "${TEST_SCHEMAS[@]}"; do
    echo "  Migrating '${schema}' schema..."
    run_flyway_migration "$schema" "$SVC_LOCATIONS" "$SVC_VOLUMES"
    echo -e "${GREEN}  ${schema} schema complete${NC}"
  done

  echo ""
  echo -e "${GREEN}======================================================${NC}"
  echo -e "${GREEN}Pre-build setup complete!${NC}"
  echo -e "${GREEN}======================================================${NC}"
  echo ""
  echo "Configuration:"
  echo "  AWS_REGION: $AWS_REGION"
  echo "  AccountId: $AccountId"
  echo "  EnvironmentNameLower: $EnvironmentNameLower"
  echo "  ResourceName: $ResourceName"
  echo "  DatabaseName: $DatabaseName"
  echo "  DatabaseEndpoint: $DatabaseEndpoint:$DatabasePort"
  echo "  IndexDomainEndpoint: $IndexDomainScheme://$IndexDomainEndpoint"
  echo ""
}

# Run and clean up
_prebuild_main
_prebuild_rc=$?
unset -f _prebuild_main
return $_prebuild_rc 2>/dev/null || exit $_prebuild_rc
