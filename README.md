# SQL WebUI

A powerful web-based SQL management tool with CSV/Excel import/export, batch scheduling, and data visualization capabilities.

## Features

- **SQL Editor**: Execute SQL queries with syntax highlighting
- **CSV/Excel Import**: Import data from CSV and Excel files
- **Data Export**: Export query results to various formats
- **Authentication**: Secure user authentication system
- **Docker Support**: Easy deployment with Docker Compose

## Tech Stack

- **Backend**: Python, FastAPI, SQLAlchemy, PostgreSQL
- **Frontend**: React, TypeScript, Material-UI
- **Infrastructure**: Docker, Docker Compose, Nginx

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Node.js 20+ (for local development)
- Python 3.11+ (for local development)

### Running with Docker

1. Clone the repository:
```bash
git clone <repository-url>
cd csv2sql-webui
```

2. Create environment files:
```bash
cp backend/.env.example backend/.env
```

3. Start the application:
```bash
docker-compose up -d
```

4. Access the application:
- Frontend: http://localhost
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

### Development Setup

#### Backend Development

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

#### Frontend Development

```bash
cd frontend
# Install pnpm if not already installed
corepack enable && corepack prepare pnpm@latest --activate
# Install dependencies
pnpm install
# Start dev server
pnpm start
```

### Creating Initial User

To create an initial admin user, run:

```bash
docker-compose exec backend python -m app.scripts.create_admin
```

Or via API:
```bash
curl -X POST "http://localhost:8000/api/v1/users/" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@example.com",
    "username": "admin",
    "password": "adminpassword",
    "full_name": "Admin User",
    "is_superuser": true
  }'
```

## Project Structure

```
csv2sql-webui/
├── backend/
│   ├── app/
│   │   ├── api/         # API endpoints
│   │   ├── core/        # Core configuration
│   │   ├── db/          # Database configuration
│   │   ├── models/      # SQLAlchemy models
│   │   ├── schemas/     # Pydantic schemas
│   │   └── services/    # Business logic
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/  # React components
│   │   ├── contexts/    # React contexts
│   │   ├── pages/       # Page components
│   │   └── services/    # API services
│   ├── Dockerfile
│   └── package.json
├── database/
└── docker-compose.yml
```

## API Documentation

Once the backend is running, you can access the interactive API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.
