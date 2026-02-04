#!/usr/bin/env bash
# Build StarRocks format-sdk C++ wrapper on macOS (JNI shared library only)

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FORMAT_SDK_DIR="${ROOT_DIR}/format-sdk"
CMAKE_SOURCE_DIR="${SCRIPT_DIR}/format-sdk"

BUILD_ROOT="${BUILD_DIR:-$SCRIPT_DIR/build-format-sdk}"
BUILD_TYPE="Release"
CLEAN_BUILD=0
WITH_STARCACHE=0
USE_STAROS=0
PARALLEL_JOBS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
FORMAT_LIB_OVERRIDE=""

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
StarRocks format-sdk C++ wrapper builder for macOS ARM64

Usage: $(basename "$0") [OPTIONS]

Options:
    -h, --help              Show this help message
    --clean                 Remove build directory before configuring
    --release               Build in Release mode (default)
    --debug                 Build in Debug mode
    --with-starcache        Enable starcache support (default: OFF)
    --with-staros           Enable staros code path (default: OFF)
    --format-lib PATH       Path to libstarrocks_format.dylib
    --build-dir DIR         Build directory root (default: $BUILD_ROOT)
    --parallel N            Set parallel jobs (default: $PARALLEL_JOBS)

Environment Variables:
    STARROCKS_HOME          StarRocks root directory (default: $ROOT_DIR)
    STARROCKS_THIRDPARTY    Third-party directory (default: \$STARROCKS_HOME/thirdparty)
    JAVA_HOME               JDK path (used for JNI headers)

Example:
    $(basename "$0") --clean --release
    $(basename "$0") --debug --format-lib /path/to/libstarrocks_format.dylib
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
        --with-starcache)
            WITH_STARCACHE=1
            shift
            ;;
        --with-staros)
            USE_STAROS=1
            shift
            ;;
        --format-lib)
            FORMAT_LIB_OVERRIDE="$2"
            shift 2
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

log_info "=== StarRocks format-sdk C++ wrapper builder (macOS) ==="
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

if [[ ! -d "$FORMAT_SDK_DIR" ]]; then
    log_error "Format SDK directory not found: $FORMAT_SDK_DIR"
    exit 1
fi

if [[ ! -f "$CMAKE_SOURCE_DIR/CMakeLists.txt" ]]; then
    log_error "CMakeLists.txt not found: $CMAKE_SOURCE_DIR/CMakeLists.txt"
    exit 1
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
    if [[ -x "/usr/libexec/java_home" ]]; then
        JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
        export JAVA_HOME
    fi
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
    log_error "JAVA_HOME is not set. Please set JAVA_HOME to a JDK path."
    exit 1
fi

if [[ ! -f "${ROOT_DIR}/gensrc/build" && ! -d "${ROOT_DIR}/gensrc/build" ]]; then
    log_error "gensrc/build not found. Run: (cd ${ROOT_DIR}/gensrc && make)"
    exit 1
fi

FORMAT_LIB_PATH=""
if [[ -n "$FORMAT_LIB_OVERRIDE" ]]; then
    FORMAT_LIB_PATH="$FORMAT_LIB_OVERRIDE"
else
    for candidate in \
        "${ROOT_DIR}/be/output/format-lib/libstarrocks_format.dylib" \
        "${ROOT_DIR}/output/format-lib/libstarrocks_format.dylib"
    do
        if [[ -f "$candidate" ]]; then
            FORMAT_LIB_PATH="$candidate"
            break
        fi
    done
fi

if [[ -z "$FORMAT_LIB_PATH" || ! -f "$FORMAT_LIB_PATH" ]]; then
    log_error "libstarrocks_format.dylib not found. Use --format-lib to specify the path."
    exit 1
fi

if [[ $CLEAN_BUILD -eq 1 ]]; then
    log_info "Cleaning build directory: $BUILD_ROOT/$BUILD_TYPE"
    rm -rf "$BUILD_ROOT/$BUILD_TYPE"
fi

mkdir -p "$BUILD_ROOT/$BUILD_TYPE"

log_info "Configuring CMake..."
cmake -S "$CMAKE_SOURCE_DIR" \
      -B "$BUILD_ROOT/$BUILD_TYPE" \
      -G "${CMAKE_GENERATOR:-Ninja}" \
      -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
      -DTARGET_NAME=starrocks_format_wrapper \
      -DSTARROCKS_FORMAT_LIB="$FORMAT_LIB_PATH" \
      -DWITH_STARCACHE="$( [[ $WITH_STARCACHE -eq 1 ]] && echo ON || echo OFF )" \
      -DUSE_STAROS="$( [[ $USE_STAROS -eq 1 ]] && echo ON || echo OFF )"

log_info "Building..."
cmake --build "$BUILD_ROOT/$BUILD_TYPE" --config "$BUILD_TYPE" -- -j"$PARALLEL_JOBS"

WRAPPER_LIB="libstarrocks_format_wrapper.dylib"
WRAPPER_PATH="$BUILD_ROOT/$BUILD_TYPE/$WRAPPER_LIB"
if [[ ! -f "$WRAPPER_PATH" ]]; then
    log_error "Wrapper library not found: $WRAPPER_PATH"
    exit 1
fi

OUTPUT_DIR="$FORMAT_SDK_DIR/target/classes/native"
mkdir -p "$OUTPUT_DIR"
cp -f "$WRAPPER_PATH" "$OUTPUT_DIR/"
cp -f "$FORMAT_LIB_PATH" "$OUTPUT_DIR/"

log_success "Built: $WRAPPER_PATH"
log_success "Copied native libs to: $OUTPUT_DIR"
