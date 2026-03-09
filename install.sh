#!/bin/sh
# install.sh — download and install the stophammer binary for your Linux architecture
# Usage: curl -fsSL https://raw.githubusercontent.com/stophammer/stophammer/main/install.sh | sh
#        or with dry-run: sh install.sh --dry-run

set -e

REPO="stophammer/stophammer"
INSTALL_DIR="/usr/local/bin"
BINARY_NAME="stophammer"
DRY_RUN=0

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --dry-run)
            DRY_RUN=1
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $0 [--dry-run]" >&2
            exit 1
            ;;
    esac
done

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)
        ASSET="stophammer-linux-x86_64"
        ;;
    aarch64|arm64)
        ASSET="stophammer-linux-aarch64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH" >&2
        echo "stophammer provides binaries for x86_64 and aarch64 only." >&2
        exit 1
        ;;
esac

# Detect latest release tag from GitHub API (no jq required — plain text parsing)
RELEASES_URL="https://api.github.com/repos/${REPO}/releases/latest"
echo "Fetching latest release from ${RELEASES_URL} ..."
TAG="$(curl -fsSL "$RELEASES_URL" \
    | tr ',' '\n' \
    | grep '"tag_name"' \
    | head -1 \
    | sed 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')"

if [ -z "$TAG" ]; then
    echo "Failed to determine the latest release tag." >&2
    exit 1
fi

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"
INSTALL_PATH="${INSTALL_DIR}/${BINARY_NAME}"

echo ""
echo "  Architecture : $ARCH"
echo "  Asset        : $ASSET"
echo "  Release tag  : $TAG"
echo "  Download URL : $DOWNLOAD_URL"
echo "  Install path : $INSTALL_PATH"
echo ""

if [ "$DRY_RUN" -eq 1 ]; then
    echo "[dry-run] Would download $DOWNLOAD_URL"
    echo "[dry-run] Would install to $INSTALL_PATH"
    echo "[dry-run] No changes made."
    exit 0
fi

# Download to a temporary file
TMP="$(mktemp)"
echo "Downloading $ASSET ..."
curl -fsSL -o "$TMP" "$DOWNLOAD_URL"
chmod +x "$TMP"

# Install — use sudo only if we cannot write to the target directory directly
if [ -w "$INSTALL_DIR" ]; then
    mv "$TMP" "$INSTALL_PATH"
else
    echo "Escalating with sudo to write to $INSTALL_DIR ..."
    sudo mv "$TMP" "$INSTALL_PATH"
fi

echo ""
echo "stophammer $TAG installed to $INSTALL_PATH"
echo ""
echo "Next steps:"
echo "  1. Set required environment variable:"
echo "       export CRAWL_TOKEN=<your-secret-token>"
echo ""
echo "  2. Optional — override defaults:"
echo "       export DB_PATH=/var/lib/stophammer/stophammer.db"
echo "       export KEY_PATH=/var/lib/stophammer/signing.key"
echo "       export BIND=0.0.0.0:8008"
echo ""
echo "  3. Run stophammer:"
echo "       stophammer"
echo ""
echo "For production deployments, see stophammer.service and stophammer.env.example"
echo "in the repository for a systemd unit and environment file template."
