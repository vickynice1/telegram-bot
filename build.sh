#!/bin/bash
set -e

echo "🔧 Setting up Python environment..."
python -m pip install --upgrade pip setuptools wheel

echo "📦 Installing dependencies with compatibility flags..."
pip install --only-binary=:all: --no-build-isolation -r requirements.txt

echo "✅ Build completed successfully!"
