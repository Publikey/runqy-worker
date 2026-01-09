#!/bin/sh
set -e

# runqy-worker installer script
# Usage: curl -fsSL https://raw.githubusercontent.com/publikey/runqy-worker/main/install.sh | sh
#
# Environment variables:
#   VERSION      - Specific version to install (default: latest)
#   INSTALL_DIR  - Installation directory (default: /usr/local/bin)

REPO="publikey/runqy-worker"
BINARY_NAME="runqy-worker"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Colors (if terminal supports)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() { printf "${GREEN}[INFO]${NC} %s\n" "$1"; }
warn() { printf "${YELLOW}[WARN]${NC} %s\n" "$1"; }
error() { printf "${RED}[ERROR]${NC} %s\n" "$1"; exit 1; }
header() { printf "${BLUE}==>${NC} %s\n" "$1"; }

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *) error "Unsupported OS: $(uname -s)" ;;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        armv7l) echo "arm" ;;
        *) error "Unsupported architecture: $(uname -m)" ;;
    esac
}

# Get latest release version from GitHub
get_latest_version() {
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" |
        grep '"tag_name":' |
        sed -E 's/.*"([^"]+)".*/\1/'
}

# Download and install
install() {
    header "runqy-worker installer"

    OS=$(detect_os)
    ARCH=$(detect_arch)

    if [ -z "$VERSION" ]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
        if [ -z "$VERSION" ]; then
            error "Failed to get latest version. Please set VERSION manually."
        fi
    fi

    info "Installing ${BINARY_NAME} ${VERSION} for ${OS}/${ARCH}"

    # Construct download URL
    # Version without 'v' prefix for archive name
    VERSION_NUM="${VERSION#v}"
    ARCHIVE_NAME="${BINARY_NAME}_${VERSION_NUM}_${OS}_${ARCH}"

    if [ "$OS" = "windows" ]; then
        ARCHIVE_NAME="${ARCHIVE_NAME}.zip"
    else
        ARCHIVE_NAME="${ARCHIVE_NAME}.tar.gz"
    fi

    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE_NAME}"

    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap "rm -rf ${TMP_DIR}" EXIT

    info "Downloading from ${DOWNLOAD_URL}"
    if ! curl -fsSL "${DOWNLOAD_URL}" -o "${TMP_DIR}/${ARCHIVE_NAME}"; then
        error "Failed to download ${DOWNLOAD_URL}"
    fi

    # Extract
    info "Extracting..."
    cd "${TMP_DIR}"
    if [ "$OS" = "windows" ]; then
        unzip -q "${ARCHIVE_NAME}"
    else
        tar xzf "${ARCHIVE_NAME}"
    fi

    # Install
    info "Installing to ${INSTALL_DIR}"
    if [ -w "${INSTALL_DIR}" ]; then
        mv "${BINARY_NAME}" "${INSTALL_DIR}/"
    else
        warn "Need sudo to write to ${INSTALL_DIR}"
        sudo mv "${BINARY_NAME}" "${INSTALL_DIR}/"
    fi

    chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

    echo ""
    header "Installation complete!"
    info "Binary: ${INSTALL_DIR}/${BINARY_NAME}"
    info "Version: ${VERSION}"
    echo ""
    info "Next steps:"
    echo "  1. Create a config file:"
    echo "     curl -fsSL https://raw.githubusercontent.com/${REPO}/main/config.yml.example -o config.yml"
    echo ""
    echo "  2. Edit config.yml with your Redis and handler settings"
    echo ""
    echo "  3. Run the worker:"
    echo "     ${BINARY_NAME} --config config.yml"
    echo ""
    info "Run '${BINARY_NAME} --version' to verify installation"
}

# Verify checksum (optional)
verify_checksum() {
    if command -v sha256sum > /dev/null; then
        info "Verifying checksum..."
        CHECKSUM_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"
        if curl -fsSL "${CHECKSUM_URL}" -o checksums.txt 2>/dev/null; then
            if sha256sum -c --ignore-missing checksums.txt 2>/dev/null; then
                info "Checksum verified"
            else
                warn "Checksum verification failed"
            fi
        fi
    fi
}

install
