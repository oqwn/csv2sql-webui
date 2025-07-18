# SQL Web UI - Architecture Design

## Overview
A comprehensive web-based SQL interface with data import/export capabilities, business intelligence features, and advanced security controls.

## Current Implementation Status

### Completed (Phase 0-1)
- ✅ Docker infrastructure with multi-service setup
- ✅ Python/FastAPI backend with authentication
- ✅ React/TypeScript frontend with Material-UI
- ✅ PostgreSQL database integration
- ✅ JWT-based authentication system
- ✅ Basic SQL query execution
- ✅ CSV import functionality

### Technology Stack (Implemented)

#### Frontend
- **Framework**: React 18 with TypeScript
- **UI Library**: Material-UI (MUI) v5
- **State Management**: React Context API + React Query (TanStack Query)
- **Routing**: React Router v6
- **HTTP Client**: Axios
- **Build Tool**: Vite 5 (replaced Create React App)

#### Backend
- **Framework**: Python 3.11 with FastAPI
- **ORM**: SQLAlchemy 2.0
- **Authentication**: JWT (python-jose)
- **Password Hashing**: Passlib with bcrypt
- **Data Processing**: Pandas, OpenPyXL
- **ASGI Server**: Uvicorn

#### Infrastructure
- **Database**: PostgreSQL 15
- **Cache/Queue**: Redis 7
- **Task Queue**: Celery 5 (configured)
- **Web Server**: Nginx (reverse proxy)
- **Containerization**: Docker & Docker Compose

### Database
- **Primary**: PostgreSQL (main application database)
- **Cache**: Redis (session management, job queues)
- **Search**: Elasticsearch (full-text search for data catalog)
- **Time Series**: InfluxDB (optional, for metrics)

### Infrastructure
- **Container**: Docker
- **Orchestration**: Kubernetes (production)
- **Message Queue**: RabbitMQ/Apache Kafka
- **Object Storage**: MinIO/S3 (file uploads)

## Current System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Client Browser                               │
└──────────────────────────────┬──────────────────────────────────┘
                               │ HTTP/HTTPS
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Nginx Reverse Proxy (Port 80)                  │
└────────────┬──────────────────────────────┬─────────────────────┘
             │ /api/*                       │ /*
             ▼                              ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│  FastAPI Backend        │    │   React Frontend        │
│    (Port 8000)          │    │    (Port 3000/80)       │
│                         │    │                         │
│  ✅ Authentication      │    │  ✅ Material-UI         │
│  ✅ SQL Execution       │    │  ✅ Protected Routes    │
│  ✅ CSV Import          │    │  ✅ Auth Context        │
│  ⬜ Excel Import        │    │  ✅ SQL Editor          │
│  ⬜ Batch Scheduling    │    │  ⬜ BI Dashboard        │
└────────┬────────────────┘    └─────────────────────────┘
         │
         ├──────────────┬──────────────┐
         ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ PostgreSQL  │ │    Redis    │ │   Celery    │
│  Database   │ │    Cache    │ │  (Workers)  │
│    ✅       │ │     ✅      │ │     ⬜      │
└─────────────┘ └─────────────┘ └─────────────┘
```

## Planned Architecture (Full Implementation)

```
┌─────────────────────────────────────────────────────────────┐
│                         Frontend                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐   │
│  │   SQL   │  │   BI    │  │  Data   │  │    Admin    │   │
│  │ Editor  │  │Dashboard│  │ Catalog │  │   Console   │   │
│  └────┬────┘  └────┬────┘  └────┬────┘  └──────┬──────┘   │
│       └────────────┴────────────┴───────────────┘          │
│                            │                                │
└────────────────────────────┼────────────────────────────────┘
                             │ HTTP/WebSocket
┌────────────────────────────┼────────────────────────────────┐
│                      API Gateway                             │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐  │
│  │Authentication│  │Rate Limiting│  │  Load Balancer   │  │
│  └──────────────┘  └─────────────┘  └──────────────────┘  │
└────────────────────────────┼────────────────────────────────┘
                             │
┌────────────────────────────┼────────────────────────────────┐
│                    Backend Services                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │   Query    │  │   Import/  │  │    Job     │           │
│  │  Engine    │  │   Export   │  │ Scheduler  │           │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘           │
│        │               │               │                    │
│  ┌─────┴──────┐  ┌─────┴──────┐  ┌─────┴──────┐           │
│  │   Data     │  │    File    │  │   Queue    │           │
│  │  Catalog   │  │  Processor │  │  Manager   │           │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘           │
│        │               │               │                    │
│  ┌─────┴──────┐  ┌─────┴──────┐  ┌─────┴──────┐           │
│  │  Security  │  │    LLM     │  │   NoSQL    │           │
│  │   Engine   │  │Integration │  │  Adapter   │           │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘           │
└────────┼───────────────┼───────────────┼────────────────────┘
         │               │               │
┌────────┼───────────────┼───────────────┼────────────────────┐
│                    Data Layer                                │
│  ┌─────┴──────┐  ┌─────┴──────┐  ┌─────┴──────┐           │
│  │PostgreSQL  │  │   Redis    │  │Elasticsearch│           │
│  └────────────┘  └────────────┘  └────────────┘           │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │  MongoDB   │  │   MinIO    │  │  InfluxDB  │           │
│  └────────────┘  └────────────┘  └────────────┘           │
└──────────────────────────────────────────────────────────────┘
```

## Recent Architecture Updates

### Frontend Build Tool Migration (Vite)
We migrated from Create React App to Vite for the following benefits:
- **Faster Development**: Instant server start and hot module replacement
- **Better Performance**: Native ES modules in development
- **Modern Tooling**: Built-in TypeScript support and optimized production builds
- **Simplified Configuration**: Less boilerplate and easier customization

Key changes:
- Environment variables now use `VITE_` prefix instead of `REACT_APP_`
- Configuration in `vite.config.ts` instead of webpack config
- `index.html` moved to project root
- Direct import of TypeScript files without compilation step in dev

## Current Implementation Details

### Backend Structure (FastAPI)
```
backend/
├── app/
│   ├── api/
│   │   ├── deps.py              # Common dependencies
│   │   └── v1/
│   │       ├── api.py           # API router aggregation
│   │       └── endpoints/       # API endpoints
│   │           ├── auth.py      # Authentication endpoints
│   │           ├── users.py     # User management
│   │           ├── sql.py       # SQL execution
│   │           └── csv_import.py # CSV import
│   ├── core/
│   │   ├── config.py            # Application settings
│   │   └── security.py          # JWT & password hashing
│   ├── db/
│   │   ├── base.py              # SQLAlchemy base
│   │   └── session.py           # Database session
│   ├── models/
│   │   └── user.py              # User model
│   ├── schemas/
│   │   ├── user.py              # User Pydantic schemas
│   │   ├── token.py             # Auth token schemas
│   │   └── sql.py               # SQL query/result schemas
│   └── services/
│       ├── auth.py              # Authentication logic
│       ├── user.py              # User service
│       ├── sql_executor.py      # SQL execution service
│       └── csv_importer.py      # CSV import service
└── main.py                      # FastAPI application entry
```

### Frontend Structure (React)
```
frontend/
└── src/
    ├── components/
    │   ├── auth/
    │   │   └── ProtectedRoute.tsx  # Route protection
    │   └── common/
    │       └── Layout.tsx           # Main layout with navigation
    ├── contexts/
    │   └── AuthContext.tsx          # Authentication context
    ├── pages/
    │   ├── LoginPage.tsx            # Login interface
    │   ├── DashboardPage.tsx        # Main dashboard
    │   ├── SQLEditorPage.tsx        # SQL query interface
    │   └── ImportPage.tsx           # CSV import interface
    └── services/
        └── api.ts                   # API client with interceptors
```

### Security Implementation
- **Authentication**: JWT tokens with expiration
- **Password Storage**: bcrypt hashing
- **API Security**: Bearer token authentication
- **CORS**: Configured for frontend access
- **Input Validation**: Pydantic schemas for all inputs

## Component Details (Planned)

### 1. SQL Grammar & Query Engine
- **Parser**: ANTLR4 or PEG.js for SQL parsing
- **Validator**: Schema-aware validation
- **Optimizer**: Query optimization before execution
- **Executor**: Multi-database query execution

### 2. Import/Export Service
- **File Handlers**: 
  - CSV: Papa Parse or custom streamer
  - Excel: SheetJS or openpyxl
- **Type Detection**: Automatic column type inference
- **Streaming**: Large file handling with streams
- **Progress Tracking**: WebSocket-based progress updates

### 3. Batch Scheduling System
- **Scheduler**: Cron-based scheduling with node-cron/APScheduler
- **Queue**: Redis-backed job queue (Bull/Celery)
- **Workers**: Distributed worker pools
- **Monitoring**: Job status tracking and alerts

### 4. Data Catalog
- **Metadata Store**: PostgreSQL with JSONB for flexible schema
- **Search Engine**: Elasticsearch for full-text search
- **Version Control**: Git-like versioning for schema changes
- **Lineage Tracking**: Graph database (Neo4j) for data lineage

### 5. BI Dashboard
- **Visualization Engine**: D3.js for custom charts
- **Layout System**: Grid-based drag-and-drop
- **Data Pipeline**: Real-time data streaming
- **Export**: PDF/PNG generation service

### 6. Security Layer
- **RBAC**: Role-based access control with Casbin
- **Row-Level Security**: Policy-based filtering
- **Column-Level Security**: Dynamic data masking
- **Audit**: Comprehensive logging with correlation IDs

### 7. LLM Integration
- **API Gateway**: OpenAI/Anthropic API integration
- **Prompt Engineering**: Template-based prompts
- **Context Management**: Vector database for context
- **Safety**: Input validation and output filtering

### 8. NoSQL Support
- **Adapters**: Database-specific query translators
- **Unified Interface**: Common query API
- **Schema Discovery**: Automatic schema inference

## Security Architecture

### Authentication & Authorization
- **JWT**: Token-based authentication
- **OAuth2**: Social login support
- **SAML**: Enterprise SSO integration
- **MFA**: Two-factor authentication

### Data Security
- **Encryption**: TLS 1.3 for transport, AES-256 for storage
- **Key Management**: HashiCorp Vault or AWS KMS
- **Data Masking**: Dynamic masking based on roles
- **Audit Trail**: Immutable audit logs

## Scalability Considerations

### Horizontal Scaling
- **Stateless Services**: All backend services are stateless
- **Load Balancing**: HAProxy/Nginx for distribution
- **Database Sharding**: Partition by tenant/dataset
- **Caching**: Multi-level caching strategy

### Performance Optimization
- **Query Caching**: Redis-based result caching
- **Connection Pooling**: Database connection management
- **Lazy Loading**: On-demand data loading
- **CDN**: Static asset distribution

## Docker Architecture

### Container Strategy
- **Multi-stage builds**: Minimize image size
- **Base images**: Alpine Linux for security and size
- **Layer caching**: Optimize build times
- **Health checks**: Ensure service availability
- **Non-root users**: Security best practice

### Service Containers
1. **Frontend Container**
   - Node.js build stage
   - Nginx serving stage
   - Static asset optimization

2. **Backend Container**
   - Node.js/Python runtime
   - Application dependencies
   - Environment configuration

3. **Worker Container**
   - Job processing
   - Scheduled tasks
   - Background operations

4. **Database Containers**
   - PostgreSQL with persistent volumes
   - Redis for caching/queues
   - Elasticsearch for search
   - MongoDB for NoSQL support

### Current Docker Implementation

Our current `docker-compose.yml` provides the following services:

```yaml
version: '3.8'

services:
  db:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: sqlwebui
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  backend:
    build:
      context: ./backend
      dockerfile: Dockerfile
    environment:
      DATABASE_URL: postgresql://postgres:postgres@db:5432/sqlwebui
      REDIS_URL: redis://redis:6379/0
      SECRET_KEY: ${SECRET_KEY:-your-secret-key-here-change-in-production}
      BACKEND_CORS_ORIGINS: '["http://localhost:3000","http://localhost:8000","http://localhost"]'
    volumes:
      - ./backend:/app
    ports:
      - "8000:8000"
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_healthy
    command: uvicorn main:app --host 0.0.0.0 --port 8000 --reload

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile
    ports:
      - "80:80"
    depends_on:
      - backend
    environment:
      REACT_APP_API_URL: http://localhost:8000/api/v1

  # Development-only service for React hot-reloading
  frontend-dev:
    image: node:18-alpine
    working_dir: /app
    volumes:
      - ./frontend:/app
      - /app/node_modules
    ports:
      - "3000:3000"
    environment:
      REACT_APP_API_URL: http://localhost:8000/api/v1
    command: npm start
    profiles:
      - dev

volumes:
  postgres_data:
```

#### Production Environment
```yaml
version: '3.8'

services:
  frontend:
    image: ${DOCKER_REGISTRY}/sqlwebui-frontend:${VERSION}
    deploy:
      replicas: 3
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    environment:
      - NODE_ENV=production
      - API_URL=${API_URL}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  backend:
    image: ${DOCKER_REGISTRY}/sqlwebui-backend:${VERSION}
    deploy:
      replicas: 5
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    environment:
      - NODE_ENV=production
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - JWT_SECRET=${JWT_SECRET}
    secrets:
      - db_password
      - jwt_secret
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  worker:
    image: ${DOCKER_REGISTRY}/sqlwebui-worker:${VERSION}
    deploy:
      replicas: 3
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    environment:
      - NODE_ENV=production
    secrets:
      - db_password
      - redis_password

secrets:
  db_password:
    external: true
  jwt_secret:
    external: true
  redis_password:
    external: true
```

### Docker Examples

#### Frontend Dockerfile
```dockerfile
# Build stage
FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build

# Production stage
FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost/health || exit 1
```

#### Backend Dockerfile
```dockerfile
FROM node:18-alpine
RUN apk add --no-cache python3 make g++ postgresql-client
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
USER node
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1
CMD ["node", "server.js"]
```

### One-Click Deployment

#### Quick Start Script
```bash
#!/bin/bash
# deploy.sh - One-click deployment script

# Pull latest code
git pull origin main

# Build images
docker-compose -f docker-compose.prod.yml build

# Deploy services
docker-compose -f docker-compose.prod.yml up -d

# Run migrations
docker-compose -f docker-compose.prod.yml exec backend npm run migrate

# Check health
docker-compose -f docker-compose.prod.yml ps
```

#### Development Quick Start
```bash
# Clone repository
git clone https://github.com/your-org/sql-webui.git
cd sql-webui

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Access application
# Frontend: http://localhost:3000
# Backend API: http://localhost:8000
# PostgreSQL: localhost:5432
# Redis: localhost:6379
# Elasticsearch: http://localhost:9200
# MinIO: http://localhost:9001
```

### Docker Best Practices

1. **Security**
   - Run containers as non-root users
   - Use official base images
   - Scan images for vulnerabilities
   - Keep secrets out of images

2. **Performance**
   - Use multi-stage builds
   - Minimize layer count
   - Cache dependencies
   - Use .dockerignore

3. **Monitoring**
   - Health checks for all services
   - Centralized logging
   - Resource limits
   - Prometheus metrics

4. **Persistence**
   - Named volumes for data
   - Backup strategies
   - Volume drivers for cloud storage

## Deployment Strategy

### Production Deployment Options

1. **Docker Swarm**
   - Built-in orchestration
   - Simple setup
   - Good for small-medium deployments

2. **Kubernetes**
   - Advanced orchestration
   - Auto-scaling
   - Enterprise features
   - Helm charts for deployment

3. **Cloud Platforms**
   - AWS ECS/Fargate
   - Google Cloud Run
   - Azure Container Instances
   - DigitalOcean App Platform

### CI/CD Pipeline
```yaml
# .github/workflows/deploy.yml
name: Build and Deploy

on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build and push Docker images
        run: |
          docker build -t ${{ secrets.DOCKER_REGISTRY }}/sqlwebui-frontend:${{ github.sha }} ./frontend
          docker build -t ${{ secrets.DOCKER_REGISTRY }}/sqlwebui-backend:${{ github.sha }} ./backend
          docker push ${{ secrets.DOCKER_REGISTRY }}/sqlwebui-frontend:${{ github.sha }}
          docker push ${{ secrets.DOCKER_REGISTRY }}/sqlwebui-backend:${{ github.sha }}
      - name: Deploy to production
        run: |
          ssh ${{ secrets.DEPLOY_HOST }} "cd /opt/sqlwebui && ./deploy.sh ${{ github.sha }}"
```

## Current API Implementation

### Implemented RESTful Endpoints

```
# Authentication
POST   /api/v1/auth/login           # User login (returns JWT token)

# User Management  
POST   /api/v1/users/               # Create new user
GET    /api/v1/users/me             # Get current user info

# SQL Operations
POST   /api/v1/sql/execute          # Execute SQL query

# Data Import
POST   /api/v1/import/csv           # Import CSV file

# Health Check
GET    /health                      # Service health status
```

### API Request/Response Examples

#### Authentication
```json
// POST /api/v1/auth/login
{
  "username": "admin",
  "password": "password"
}

// Response
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "bearer"
}
```

#### SQL Execution
```json
// POST /api/v1/sql/execute
{
  "sql": "SELECT * FROM users LIMIT 10"
}

// Response
{
  "columns": ["id", "email", "username", "created_at"],
  "rows": [[1, "admin@example.com", "admin", "2024-01-01T00:00:00Z"]],
  "row_count": 1,
  "execution_time": 0.025
}
```

### Planned API Endpoints

```
# Phase 2-3: SQL & Export
GET    /api/v1/tables               # List database tables
GET    /api/v1/tables/{name}/schema # Get table schema
POST   /api/v1/export/csv           # Export to CSV
POST   /api/v1/export/excel         # Export to Excel

# Phase 4: Batch Scheduling  
POST   /api/v1/jobs                 # Create scheduled job
GET    /api/v1/jobs                 # List jobs
GET    /api/v1/jobs/{id}            # Get job details
DELETE /api/v1/jobs/{id}            # Delete job

# Phase 5: Data Catalog
GET    /api/v1/catalog/search       # Full-text search
POST   /api/v1/catalog/metadata     # Add metadata
PUT    /api/v1/catalog/{id}/tags    # Update tags

# Phase 6: BI Dashboard
GET    /api/v1/dashboards           # List dashboards
POST   /api/v1/dashboards           # Create dashboard
GET    /api/v1/charts/types         # Available chart types
```

## Data Flow Examples

### Query Execution Flow
1. User submits SQL query
2. API Gateway validates request
3. Query Engine parses and validates SQL
4. Security Engine applies row/column filters
5. Query executed against database
6. Results cached in Redis
7. Response sent to client

### Import Flow
1. User uploads CSV/Excel file
2. File stored in object storage
3. Import job queued
4. Worker processes file
5. Data validated and typed
6. Bulk insert to database
7. Metadata updated in catalog
8. User notified of completion

## Monitoring & Observability

### Metrics
- **Application**: Request rate, latency, errors
- **Database**: Query performance, connection pool
- **Infrastructure**: CPU, memory, disk usage

### Logging
- **Structured Logging**: JSON format
- **Log Levels**: DEBUG, INFO, WARN, ERROR
- **Correlation IDs**: Request tracing

### Alerting
- **Thresholds**: Performance degradation
- **Anomaly Detection**: Unusual patterns
- **Incident Response**: PagerDuty integration