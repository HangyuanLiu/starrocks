#!/usr/bin/env bash
# Build StarRocks format-lib on macOS (libstarrocks_format.dylib)

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FORMAT_LIB_CMAKE_DIR="${SCRIPT_DIR}/format-lib"

BUILD_ROOT="${BUILD_DIR:-$SCRIPT_DIR/build-format-lib}"
BUILD_TYPE="Release"
CLEAN_BUILD=0
PARALLEL_JOBS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
USE_AWS_SDK=1

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat << EOF
StarRocks format-lib builder for macOS ARM64

Usage: $(basename "$0") [OPTIONS]

Options:
    -h, --help              Show this help message
    --clean                 Remove build directory before configuring
    --release               Build in Release mode (default)
    --debug                 Build in Debug mode
    --use-aws-sdk           Use AWS SDK (default)
    --use-aws-stub          Build with minimal AWS stubs (S3 will NOT work)
    --build-dir DIR         Build directory root (default: $BUILD_ROOT)
    --parallel N            Set parallel jobs (default: $PARALLEL_JOBS)

Environment Variables:
    STARROCKS_HOME          StarRocks root directory (default: $ROOT_DIR)
    STARROCKS_THIRDPARTY    Third-party directory (default: \$STARROCKS_HOME/thirdparty)

Example:
    $(basename "$0") --clean --release
    $(basename "$0") --debug --use-aws-sdk
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --clean)
            CLEAN_BUILD=1
            shift
            ;;
        --release)
            BUILD_TYPE="Release"
            shift
            ;;
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --use-aws-sdk)
            USE_AWS_SDK=1
            shift
            ;;
        --use-aws-stub)
            USE_AWS_SDK=0
            shift
            ;;
        --build-dir)
            BUILD_ROOT="$2"
            shift 2
            ;;
        --parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

log_info "=== StarRocks format-lib builder (macOS) ==="
log_info "Root directory: $ROOT_DIR"
log_info "Build root: $BUILD_ROOT"
log_info "Build type: $BUILD_TYPE"
log_info "Parallel jobs: $PARALLEL_JOBS"

ENV_SCRIPT="$SCRIPT_DIR/env_macos.sh"
if [[ -f "$ENV_SCRIPT" ]]; then
    export STARROCKS_ENV_QUIET=1
    set +u
    # shellcheck source=/dev/null
    source "$ENV_SCRIPT"
    set -u
else
    log_error "Environment script not found: $ENV_SCRIPT"
    exit 1
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
    log_error "This script is only for macOS"
    exit 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
    log_error "This script is only for Apple Silicon (ARM64)"
    exit 1
fi

if [[ ! -f "$FORMAT_LIB_CMAKE_DIR/CMakeLists.txt" ]]; then
    log_error "CMakeLists.txt not found: $FORMAT_LIB_CMAKE_DIR/CMakeLists.txt"
    exit 1
fi

if [[ ! -d "${ROOT_DIR}/gensrc/build/gen_cpp" ]]; then
    log_error "gensrc/build/gen_cpp not found. Run: (cd ${ROOT_DIR}/gensrc && make)"
    exit 1
fi

if [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Cleaning build directory: $BUILD_ROOT/$BUILD_TYPE"
    rm -rf "$BUILD_ROOT/$BUILD_TYPE"
fi

mkdir -p "$BUILD_ROOT/$BUILD_TYPE"

AWS_STUB_FLAG="ON"
if [[ $USE_AWS_SDK -eq 1 ]]; then
    AWS_STUB_FLAG="OFF"
fi

log_info "Configuring CMake..."
cmake -S "$FORMAT_LIB_CMAKE_DIR" \
      -B "$BUILD_ROOT/$BUILD_TYPE" \
      -G "${CMAKE_GENERATOR:-Ninja}" \
      -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
      -DFORMAT_LIB_USE_AWS_STUB="$AWS_STUB_FLAG"

log_info "Building..."
cmake --build "$BUILD_ROOT/$BUILD_TYPE" --config "$BUILD_TYPE" --target starrocks_format -- -j"$PARALLEL_JOBS"

FORMAT_LIB_OUTPUT="${ROOT_DIR}/be/output/format-lib/libstarrocks_format.dylib"
if [[ ! -f "$FORMAT_LIB_OUTPUT" ]]; then
    log_error "format-lib not found: $FORMAT_LIB_OUTPUT"
    exit 1
fi

OUTPUT_DIR="${ROOT_DIR}/output/format-lib"
mkdir -p "$OUTPUT_DIR"
cp -f "$FORMAT_LIB_OUTPUT" "$OUTPUT_DIR/"

log_success "Built: $FORMAT_LIB_OUTPUT"
log_success "Copied to: $OUTPUT_DIR"
