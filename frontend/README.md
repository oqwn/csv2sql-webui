# SQL WebUI Frontend

Modern React frontend for SQL WebUI built with Vite, TypeScript, and Material-UI.

## Tech Stack

- **Build Tool**: Vite 5
- **Framework**: React 18.3
- **Language**: TypeScript 5.6
- **UI Library**: Material-UI v5
- **Routing**: React Router v6
- **State Management**: React Context + TanStack Query v5
- **HTTP Client**: Axios
- **Linting**: ESLint 9 with flat config
- **Package Manager**: pnpm (for performance)

## Development

### Prerequisites

- Node.js 20+
- pnpm 8+ (install with `corepack enable && corepack prepare pnpm@latest --activate`)

### Setup

1. Install pnpm (if not already installed):
```bash
corepack enable
corepack prepare pnpm@latest --activate
```

2. Install dependencies:
```bash
pnpm install
```

3. Create a `.env.local` file:
```bash
VITE_API_URL=http://localhost:8000/api/v1
```

4. Start development server:
```bash
pnpm run dev
```

The app will be available at http://localhost:3000

### Available Scripts

- `pnpm run dev` - Start development server
- `pnpm run build` - Build for production
- `pnpm run preview` - Preview production build
- `pnpm run lint` - Run ESLint
- `pnpm run typecheck` - Run TypeScript type checking
- `pnpm test` - Run tests with Vitest

## Project Structure

```
src/
├── components/      # Reusable components
│   ├── auth/       # Authentication components
│   └── common/     # Common UI components
├── contexts/       # React contexts
├── pages/          # Page components
├── services/       # API services
└── utils/          # Utility functions
```

## Docker Development

To run with Docker:

```bash
# Development with hot reload
docker-compose --profile dev up frontend-dev

# Production build
docker-compose up frontend
```

## Building for Production

```bash
npm run build
```

The production build will be in the `build/` directory.

## Environment Variables

- `VITE_API_URL` - Backend API URL (default: http://localhost:8000/api/v1)

## Key Features

- ✅ Modern build tooling with Vite
- ✅ TypeScript for type safety
- ✅ Material-UI components
- ✅ JWT authentication
- ✅ Protected routes
- ✅ API interceptors for auth
- ✅ Hot module replacement
- ✅ Optimized production builds
