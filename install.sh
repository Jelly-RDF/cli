#!/bin/bash

# This script installs the latest cli release from the GitHub repository
# Let's start by fetching the latest release information
REPO_BASE="Jelly-RDF/cli"
REPO="https://api.github.com/repos/$REPO_BASE/releases/latest"
RELEASE_INFO=$(curl -s $REPO)

# And let's get the tag name of the latest release
TAG_NAME=$(echo $RELEASE_INFO | grep -o '"tag_name": "[^"]*' | sed 's/"tag_name": "//')
if [ -z "$TAG_NAME" ]; then
  echo "Error: Could not fetch the latest release tag name."
  exit 1
fi

# Check the operating system and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
# Map the architecture to the appropriate binary name
case $OS in
  linux) BINARY_NAME="jelly-cli-linux" ;;
  darwin) BINARY_NAME="jelly-cli-macos" ;;
  *) echo "Unsupported operating system: $OS"; exit 1 ;;
esac
# Append the architecture to the binary name but yell for cases we don't support
case $ARCH in
  x86_64) ARCH_NAME="-x86_64"
        # check that os not darwin here
        if [ "$OS" = "darwin" ]; then
          echo "Unsupported architecture: $ARCH on macOS"
          exit 1
        fi;;
  aarch64)
    ARCH_NAME="-arm64" ;;
  arm64)
      ARCH_NAME="-arm64" ;;
  *) echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Check the installation directory
INSTALL_DIR="$HOME/.local/bin"
if [ ! -d "$INSTALL_DIR" ]; then
  echo "Creating installation directory: $INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"
fi

# Ensure that the installation directory is in the PATH
if ! echo "$PATH" | grep -q "$INSTALL_DIR"; then
  echo "Adding $INSTALL_DIR to PATH"
  # Check shell config file from bash and zsh
  if expr "$SHELL" : '.*zsh' >/dev/null; then
    echo "export PATH=\"\$PATH:$INSTALL_DIR\"" >> "$HOME/.zshrc"
  elif expr "$SHELL" : '.*bash' >/dev/null; then
    echo "export PATH=\"\$PATH:$INSTALL_DIR\"" >> "$HOME/.bashrc"
  fi
fi

# Add the binary to the PATH for the current session
export PATH="$PATH:$INSTALL_DIR"


# Download the binary
DOWNLOAD_URL="https://github.com/$REPO_BASE/releases/download/$TAG_NAME/$BINARY_NAME$ARCH_NAME"
echo "Downloading $BINARY_NAME from $DOWNLOAD_URL"
curl -L "$DOWNLOAD_URL" -o "$INSTALL_DIR/jelly-cli"
chmod +x "$INSTALL_DIR/jelly-cli"


# Link the binary to the installation directory
if [[ -f "$INSTALL_DIR/jelly-cli" ]]; then
  echo "Installation successful! You can now use Jelly CLI by running 'jelly-cli'."
else
  echo "Error: Installation failed. The binary was not found in the installation directory."
  exit 1
fi

# Check if zsh or bash is being used and source the appropriate file
if expr "$SHELL" : '.*zsh' >/dev/null; then
  echo  "Run source ""$HOME/.zshrc"" or restart your terminal to apply changes."
elif expr "$SHELL" : '.*bash' >/dev/null; then
  echo  "Run source ""$HOME/.bashrc"" or restart your terminal to apply changes."
fi


