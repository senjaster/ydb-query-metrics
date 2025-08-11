#!/bin/bash

# Development entry point for ydb-query-metrics
# This script:
# 1. Creates a virtual environment if it doesn't exist
# 2. Installs dependencies if needed
# 3. Runs the package in development mode
# 4. Passes all arguments to the CLI

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$SCRIPT_DIR/src"
VENV_DIR="$SCRIPT_DIR/venv"
REQUIREMENTS_FILE="$SCRIPT_DIR/requirements.txt"

# Check if virtual environment exists, create if it doesn't
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    
    # Activate the virtual environment
    source "$VENV_DIR/bin/activate"
    
    # Install dependencies
    echo "Installing dependencies..."
    pip install -r "$REQUIREMENTS_FILE"
    
    echo "Virtual environment setup complete."
else
    # Activate the virtual environment
    source "$VENV_DIR/bin/activate"
fi

# Check if the package is installed in development mode
if ! pip list | grep -q "ydb-query-metrics"; then
    echo "Installing package in development mode..."
    pip install -e "$SCRIPT_DIR"
fi

# Run the CLI with all arguments
ydb-query-metrics "$@"
