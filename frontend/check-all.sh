#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Running comprehensive checks for SQL WebUI Frontend..."
echo ""

# Check Node version
echo "📌 Checking Node.js version..."
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 20 ]; then
    echo -e "${RED}❌ Error: Node.js 20+ is required. Current version: $(node -v)${NC}"
    exit 1
else
    echo -e "${GREEN}✅ Node.js version: $(node -v)${NC}"
fi

# Check if pnpm is available
echo ""
echo "📌 Checking pnpm..."
if ! command -v pnpm &> /dev/null; then
    echo -e "${YELLOW}⚠️  pnpm not found. Installing via corepack...${NC}"
    corepack enable
    corepack prepare pnpm@latest --activate
fi
echo -e "${GREEN}✅ pnpm version: $(pnpm -v)${NC}"

# Check if dependencies are installed
echo ""
echo "📌 Checking dependencies..."
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}⚠️  node_modules not found. Installing dependencies...${NC}"
    pnpm install
elif [ ! -f "pnpm-lock.yaml" ]; then
    echo -e "${YELLOW}⚠️  pnpm-lock.yaml not found. Running pnpm install...${NC}"
    pnpm install
else
    echo -e "${GREEN}✅ Dependencies are installed${NC}"
fi

# Run TypeScript check
echo ""
echo "🔷 Running TypeScript type checking..."
if pnpm run typecheck; then
    echo -e "${GREEN}✅ TypeScript check passed${NC}"
else
    echo -e "${RED}❌ TypeScript check failed${NC}"
    exit 1
fi

# Run ESLint
echo ""
echo "🔷 Running ESLint..."
if pnpm run lint; then
    echo -e "${GREEN}✅ ESLint check passed${NC}"
else
    echo -e "${RED}❌ ESLint check failed${NC}"
    exit 1
fi

# Try to build
echo ""
echo "🔷 Testing production build..."
if pnpm run build; then
    echo -e "${GREEN}✅ Production build successful${NC}"
    # Clean up build artifacts
    rm -rf build
else
    echo -e "${RED}❌ Production build failed${NC}"
    exit 1
fi

# Run tests
echo ""
echo "🔷 Running tests..."
if pnpm test -- --run; then
    echo -e "${GREEN}✅ Tests passed${NC}"
else
    echo -e "${RED}❌ Tests failed${NC}"
    exit 1
fi

# Check for common issues
echo ""
echo "🔷 Checking for common issues..."

# Check if .env.local exists
if [ ! -f ".env.local" ]; then
    echo -e "${YELLOW}⚠️  .env.local not found. Creating with default values...${NC}"
    echo "VITE_API_URL=http://localhost:8000/api/v1" > .env.local
fi

# Check for unused dependencies
echo ""
echo "📌 Checking package.json consistency..."
if [ -f "pnpm-lock.yaml" ]; then
    echo -e "${GREEN}✅ pnpm-lock.yaml exists${NC}"
else
    echo -e "${YELLOW}⚠️  pnpm-lock.yaml is missing. Run 'pnpm install' to generate it.${NC}"
fi

echo ""
echo "======================================"
echo -e "${GREEN}✨ All checks completed!${NC}"
echo "======================================"
echo ""
echo "Summary:"
echo "  - Node.js 20+: ✅"
echo "  - pnpm installed: ✅"
echo "  - TypeScript: ✅"
echo "  - ESLint: ✅"
echo "  - Build: ✅"
echo "  - Tests: ✅"
echo ""

if [ ! -f "pnpm-lock.yaml" ]; then
    echo -e "${YELLOW}⚠️  Important: Don't forget to commit pnpm-lock.yaml!${NC}"
    echo ""
fi

echo "You can now:"
echo "  1. Run 'pnpm run dev' to start the development server"
echo "  2. Commit your changes (including pnpm-lock.yaml if newly generated)"
echo "  3. Push to trigger CI/CD pipeline"