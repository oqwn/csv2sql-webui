#!/bin/bash

echo "🚀 Installing SQL WebUI Frontend Dependencies..."

# Clean up old files
echo "📦 Cleaning up old dependencies..."
rm -rf node_modules package-lock.json

# Install dependencies
echo "📥 Installing packages..."
npm install

echo "✅ Installation complete!"
echo ""
echo "To start the development server, run:"
echo "  npm run dev"
echo ""
echo "The app will be available at http://localhost:3000"