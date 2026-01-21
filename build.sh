#!/bin/bash

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

# Error handler
handle_error() {
  local exit_code=$?
  local line_number=$1
  log_error "Build failed at line ${line_number} with exit code ${exit_code}"

  # Check for common error patterns and provide helpful messages
  if [ -f "/tmp/clojure-error.edn" ]; then
    log_error "Clojure error report found:"
    cat "/tmp/clojure-error.edn" || true
  fi

  # Look for recent error logs
  if ls /tmp/clojure-*.edn 2>/dev/null; then
    log_error "Recent Clojure error reports:"
    for error_file in /tmp/clojure-*.edn; do
      echo "=== ${error_file} ==="
      cat "${error_file}" || true
      echo ""
    done
  fi

  exit ${exit_code}
}

trap 'handle_error ${LINENO}' ERR

# Validate required environment variables
log_info "Validating environment variables..."
required_vars=("PROJECT_NAME" "ARTIFACT_ORG" "BUILD_ID")
for var in "${required_vars[@]}"; do
  if [ -z "${!var:-}" ]; then
    log_error "Required environment variable ${var} is not set"
    exit 1
  fi
  log_info "  ${var}=${!var}"
done

# Step 1: Run unit tests
log_info "Running unit tests..."
if PROJECT_NAME="api" clojure -M:test:unit 2>&1 | tee /tmp/unit-test.log; then
  log_success "Unit tests passed"
else
  log_error "Unit tests failed. Check /tmp/unit-test.log for details"
  cat /tmp/unit-test.log
  exit 1
fi

# Step 2: Setup AWS credentials
log_info "Setting up AWS credentials..."

log_info "Fetching AWS metadata token..."
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s) || {
  log_warning "Failed to fetch metadata token (may not be running on EC2)"
  TOKEN=""
}

if [ -n "$TOKEN" ]; then
  log_info "Fetching AWS region from EC2 metadata..."
  AWS_DEFAULT_REGION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s \
    http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region) || {
    log_warning "Failed to fetch region, using fallback"
    AWS_DEFAULT_REGION="${AWS_REGION:-eu-west-1}"
  }
else
  log_warning "Using fallback region"
  AWS_DEFAULT_REGION="${AWS_REGION:-eu-west-1}"
fi

export AWS_DEFAULT_REGION
export AWS_REGION=$AWS_DEFAULT_REGION
log_info "AWS Region: ${AWS_REGION}"

log_info "Getting AWS account ID..."
TARGET_ACCOUNT_ID="$(aws sts get-caller-identity | jq -r '.Account')" || {
  log_error "Failed to get AWS account ID. Is AWS CLI configured?"
  exit 1
}
export TARGET_ACCOUNT_ID
log_info "Account ID: ${TARGET_ACCOUNT_ID}"

log_info "Assuming PipelineRole..."
cred=$(aws sts assume-role \
  --role-arn "arn:aws:iam::${TARGET_ACCOUNT_ID}:role/PipelineRole" \
  --role-session-name "${PROJECT_NAME}-deployment-${RANDOM}" \
  --endpoint "https://sts.${AWS_DEFAULT_REGION}.amazonaws.com" \
  --region "${AWS_DEFAULT_REGION}") || {
  log_error "Failed to assume PipelineRole"
  exit 1
}

export AWS_ACCESS_KEY_ID=$(echo "$cred" | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "$cred" | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "$cred" | jq -r '.Credentials.SessionToken')
export AccountId=$TARGET_ACCOUNT_ID
log_success "AWS credentials configured"

# Step 3: Setup test environment
log_info "Setting up test environment..."
export EnvironmentNameLower=pipeline
export ApplicationName=dynamodb-svc
export DatabasePassword="no-secret"
export DatabaseEndpoint="127.0.0.1"
export DatabasePort="5433"
export IndexDomainScheme=http
export IndexDomainEndpoint=127.0.0.1:9200

# Step 4: Purge SQS queues
log_info "Purging all SQS queues..."
queue_count=0
for queue in $(aws sqs list-queues --query 'QueueUrls[]' --output text 2>/dev/null || echo ""); do
  log_info "  Purging queue: ${queue}"
  if aws sqs purge-queue --queue-url "$queue" 2>&1; then
    log_info "    Successfully purged"
  else
    log_warning "    Failed to purge queue (may be recently purged): ${queue}"
  fi
  queue_count=$((queue_count + 1))
done
log_info "Processed ${queue_count} SQS queues"

# Step 5: Database migrations
log_info "Running Flyway migrations..."

DB_URL="jdbc:postgresql://${DatabaseEndpoint}:5433/postgres?user=postgres"

log_info "Cleaning database schemas..."
flyway -password="${DatabasePassword}" \
  -schemas=test \
  -url="${DB_URL}" \
  -cleanDisabled="false" \
  clean || {
  log_error "Flyway clean failed"
  exit 1
}
log_success "Database cleaned"

log_info "Migrating 'test' schema..."
flyway -password="${DatabasePassword}" \
  -schemas=test \
  -url="${DB_URL}" \
  -locations="filesystem:${PWD}/sql/files/edd" \
  migrate || {
  log_error "Migration for 'test' schema failed"
  exit 1
}
log_success "Test schema migrated"

log_info "Migrating 'test' schema..."
flyway -password="${DatabasePassword}" \
  -schemas=test_local_svc \
  -url="${DB_URL}" \
  -locations="filesystem:${PWD}/modules/edd-core-view-store-postgres/migrations" \
  -X \
  migrate || {
  log_error "Migration for 'test' schema failed"
  exit 1
}
log_success "Test schema migrated"

# Step 6: Build and install main JAR
log_info "Building main JAR (build ${BUILD_ID})..."
VERSION="1.${BUILD_ID}"

if clojure -T:build jar+install \
  :app-group-id "${ARTIFACT_ORG}" \
  :app-artifact-id "${PROJECT_NAME}" \
  :app-version "\"${VERSION}\"" 2>&1 | tee /tmp/build-main.log; then
  log_success "Main JAR built successfully"
else
  log_error "Main JAR build failed. Log output:"
  cat /tmp/build-main.log
  # Check for Clojure error reports
  for error_file in /tmp/clojure-*.edn; do
    if [ -f "$error_file" ]; then
      log_error "Error report: ${error_file}"
      cat "$error_file"
    fi
  done
  exit 1
fi

log_info "Copying artifacts to /dist/release-libs/..."
cp pom.xml "/dist/release-libs/${PROJECT_NAME}-${VERSION}.jar.pom.xml"
cp "target/${PROJECT_NAME}-${VERSION}.jar" "/dist/release-libs/${PROJECT_NAME}-${VERSION}.jar"
ls -la target
log_success "Main artifacts copied"

# Step 7: Build modules
log_info "Building modules..."
log_info "Current directory: $(pwd)"
log_info "Listing all directories in modules/:"
ls -la modules/
log_info "Changing to modules directory..."
cd modules
log_info "Now in: $(pwd)"

module_count=0
skipped_count=0
log_info "Scanning for modules to build..."

for module in $(ls -1 | sort); do
  if [ ! -d "$module" ]; then
    log_warning "Skipping non-directory: ${module}"
    continue
  fi

  # Skip modules without deps.edn (not Clojure modules)
  if [ ! -f "$module/deps.edn" ]; then
    skipped_count=$((skipped_count + 1))
    log_warning "Skipping ${module} (no deps.edn found) [${skipped_count} skipped so far]"
    continue
  fi

  module_count=$((module_count + 1))
  log_info "========================================="
  log_info "Building module [$module_count]: ${module}"
  log_info "Current working directory: $(pwd)"
  log_info "========================================="

  log_info "Entering module directory: ${module}"
  cd "$module"
  log_info "Now in: $(pwd)"
  log_info "Module contents:"
  ls -la

  log_info "Module deps.edn contents:"
  cat deps.edn || log_warning "No deps.edn found"

  log_info "Resolving dependencies for ${module}..."
  log_info "Running: clojure -Stree"
  if ! clojure -Stree 2>&1 | tee "/tmp/module-${module}-deps.log"; then
    log_error "Failed to resolve dependencies for module: ${module}"
    log_error "Current directory: $(pwd)"
    log_error "Dependency resolution log:"
    cat "/tmp/module-${module}-deps.log"
    # Check for Clojure error reports
    for error_file in /tmp/clojure-*.edn; do
      if [ -f "$error_file" ]; then
        log_error "Dependency resolution error: ${error_file}"
        cat "$error_file"
      fi
    done
    exit 1
  fi
  log_success "Dependencies resolved successfully for ${module}"

  # Check if :it alias exists
  if grep -q ":it" deps.edn; then
    log_info "Running integration tests for ${module}..."
    log_info "Command: clojure -M:test:it --reporter documentation --no-capture-output"
    if ! clojure -M:test:it --reporter documentation --no-capture-output 2>&1 | tee "/tmp/module-${module}-it.log"; then
      log_error "Integration tests failed for module: ${module}"
      log_error "Current directory: $(pwd)"
      log_error "Test log:"
      cat "/tmp/module-${module}-it.log"
      exit 1
    fi
    log_success "Integration tests passed for ${module}"
  else
    log_warning "Skipping integration tests for ${module} (no :it alias found)"
  fi

  # Check if :unit alias exists
  if grep -q ":unit" deps.edn; then
    log_info "Running unit tests for ${module}..."
    log_info "Command: clojure -M:test:unit --reporter documentation --no-capture-output"
    if ! clojure -M:test:unit --reporter documentation --no-capture-output 2>&1 | tee "/tmp/module-${module}-unit.log"; then
      log_error "Unit tests failed for module: ${module}"
      log_error "Current directory: $(pwd)"
      log_error "Test log:"
      cat "/tmp/module-${module}-unit.log"
      exit 1
    fi
    log_success "Unit tests passed for ${module}"
  else
    log_warning "Skipping unit tests for ${module} (no :unit alias found)"
  fi

  log_info "Building JAR for ${module}..."
  log_info "Command: clojure -J-Dedd-core.override=${VERSION} -T:build jar+install"
  if ! clojure -J-Dedd-core.override="${VERSION}" -T:build jar+install \
    :app-group-id "${ARTIFACT_ORG}" \
    :app-artifact-id "${module}" \
    :app-version "\"${VERSION}\"" 2>&1 | tee "/tmp/module-${module}-build.log"; then
    log_error "JAR build failed for module: ${module}"
    log_error "Current directory: $(pwd)"
    log_error "Build log:"
    cat "/tmp/module-${module}-build.log"
    # Check for Clojure error reports
    for error_file in /tmp/clojure-*.edn; do
      if [ -f "$error_file" ]; then
        log_error "Build error: ${error_file}"
        cat "$error_file"
      fi
    done
    exit 1
  fi
  log_success "JAR built successfully for ${module}"

  log_info "Copying module artifacts for ${module}..."
  log_info "  pom.xml -> /dist/release-libs/${module}-${VERSION}.jar.pom.xml"
  cp pom.xml "/dist/release-libs/${module}-${VERSION}.jar.pom.xml"
  log_info "  target/${module}-${VERSION}.jar -> /dist/release-libs/${module}-${VERSION}.jar"
  cp "target/${module}-${VERSION}.jar" "/dist/release-libs/${module}-${VERSION}.jar"
  log_success "Module ${module} built and copied successfully"

  log_info "Returning to modules directory..."
  cd ..
  log_info "Back in: $(pwd)"
  log_info "Completed module [$module_count]: ${module}"
  log_info ""
done

log_info "========================================="
log_info "Module build summary:"
log_info "  Total modules scanned: $((module_count + skipped_count))"
log_info "  Modules built: ${module_count}"
log_info "  Modules skipped: ${skipped_count}"
log_info "========================================="

cd ..
log_info "Returned to project root: $(pwd)"
log_success "All ${module_count} modules built successfully"

# Step 8: Run main integration tests
log_info "========================================="
log_info "Running main integration tests from: $(pwd)"
log_info "========================================="
env | grep -E "(AWS|Database|Index|Environment)" || true

if ! clojure -M:test:it --reporter documentation --no-capture-output 2>&1 | tee /tmp/integration-test.log; then
  log_error "Integration tests failed. Log output:"
  cat /tmp/integration-test.log
  exit 1
fi
log_success "Integration tests passed"

# Step 9: Cleanup
log_info "Cleaning up build artifacts..."
rm -rf /home/build/.m2/repository
rm -rf target
log_success "Cleanup complete"

# Step 11: Show final artifacts
log_info "Final artifact tree:"
tree /dist || ls -laR /dist

log_success "========================================="
log_success "Build completed successfully!"
log_success "Version: ${VERSION}"
log_success "Modules built: ${module_count}"
log_success "========================================="
