#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$PROJECT_ROOT/proto"
python -m grpc_tools.protoc \
  -I "$PROTO_DIR" \
  --python_out="$PROJECT_ROOT" \
  --grpc_python_out="$PROJECT_ROOT" \
  "$PROTO_DIR/sentiment.proto"
