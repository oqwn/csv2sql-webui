# Project Guidelines for Claude

## Project Overview
SQL WebUI is a comprehensive web-based SQL management platform with data import/export capabilities, business intelligence features, and advanced security controls.

## Key Reminders
- Keep updating todo.md and architecture.md as you progress
- Use Python (FastAPI) for backend development
- Use React with TypeScript for frontend development
- Follow the phased approach outlined in todo.md
- Implement proper error handling and validation
- Write clean, maintainable code with appropriate comments

## Development Standards

### Backend (Python/FastAPI)
- Use type hints for all functions
- Follow PEP 8 style guidelines
- Use Pydantic for data validation
- Implement proper error handling with appropriate HTTP status codes
- Use dependency injection pattern for services
- Keep business logic in service layer, not in API endpoints

### Frontend (React/TypeScript)
- Use functional components with hooks
- Implement proper TypeScript types/interfaces
- Follow React best practices (avoid inline styles, use proper keys, etc.)
- Use Material-UI components consistently
- Implement proper loading and error states
- Keep API calls in service layer

### Database
- Use SQLAlchemy ORM for database operations
- Implement proper migrations with Alembic
- Use parameterized queries to prevent SQL injection
- Add appropriate indexes for performance

### Security
- Always validate user input
- Use JWT tokens for authentication
- Implement proper CORS configuration
- Hash passwords with bcrypt
- Never log sensitive information
- Implement rate limiting for API endpoints

### Docker
- Keep images minimal using multi-stage builds
- Use health checks for all services
- Run containers as non-root users
- Use environment variables for configuration
- Implement proper volume management for data persistence

## Current Progress
- Phase 0: Docker Infrastructure ✅ (mostly complete)
- Phase 1: Core Infrastructure ✅ (complete)
- Phase 2: SQL Grammar & Parser ⬜ (next)
- Phase 3: CSV/Excel Import/Export 🟨 (CSV import done)
- Phase 4-10: Pending

## Testing Commands
```bash
# Backend tests
cd backend
pytest

# Frontend tests  
cd frontend
npm test

# Linting
cd backend
ruff check .
mypy .

cd frontend
npm run lint
npm run typecheck
```

## Important Files
- `todo.md`: Project roadmap and task tracking
- `architecture.md`: System design and technical decisions
- `docker-compose.yml`: Service orchestration
- `backend/main.py`: API entry point
- `frontend/src/App.tsx`: Frontend entry point

## Next Steps
1. Complete remaining Phase 0 tasks (production deployment files)
2. Begin Phase 2: SQL Grammar & Parser implementation
3. Enhance CSV import with better type detection
4. Add Excel import support
5. Implement data export functionality
