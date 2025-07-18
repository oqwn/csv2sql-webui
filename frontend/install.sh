#!/bin/bash

echo "🚀 Installing SQL WebUI Frontend Dependencies..."

# Enable pnpm
echo "🔧 Setting up pnpm..."
corepack enable
corepack prepare pnpm@latest --activate

# Clean up old files
echo "📦 Cleaning up old dependencies..."
rm -rf node_modules package-lock.json pnpm-lock.yaml .pnpm-store

# Install dependencies
echo "📥 Installing packages with pnpm..."
pnpm install

echo "✅ Installation complete!"
echo ""
echo "To start the development server, run:"
echo "  pnpm run dev"
echo ""
echo "The app will be available at http://localhost:3000"