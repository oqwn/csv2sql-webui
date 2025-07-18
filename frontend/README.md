# SQL WebUI Frontend

Modern React frontend for SQL WebUI built with Vite, TypeScript, and Material-UI.

## Tech Stack

- **Build Tool**: Vite 6 (latest)
- **Framework**: React 18.3
- **Language**: TypeScript 5.6
- **UI Library**: Material-UI v6 (latest)
- **Routing**: React Router v7 (latest)
- **State Management**: React Context + TanStack Query v5
- **HTTP Client**: Axios
- **Linting**: ESLint 9 with flat config

## Development

### Prerequisites

- Node.js 18+
- npm or yarn

### Setup

1. Install dependencies:
```bash
npm install
```

2. Create a `.env.local` file:
```bash
VITE_API_URL=http://localhost:8000/api/v1
```

3. Start development server:
```bash
npm run dev
```

The app will be available at http://localhost:3000

### Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint
- `npm run typecheck` - Run TypeScript type checking
- `npm test` - Run tests with Vitest

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
