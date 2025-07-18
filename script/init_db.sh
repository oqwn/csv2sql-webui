#!/bin/bash

# SQL WebUI Database Initialization Script
# This script initializes the PostgreSQL database for the SQL WebUI application

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DB_NAME="sqlwebui"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"
ADMIN_USER=$(whoami)

echo -e "${YELLOW}🚀 Starting SQL WebUI Database Initialization${NC}"

# Function to check if PostgreSQL is running
check_postgresql() {
    echo -e "${YELLOW}📋 Checking PostgreSQL status...${NC}"
    
    if brew services list | grep -q "postgresql.*started"; then
        echo -e "${GREEN}✅ PostgreSQL is running${NC}"
        return 0
    else
        echo -e "${RED}❌ PostgreSQL is not running${NC}"
        echo -e "${YELLOW}🔄 Starting PostgreSQL...${NC}"
        
        # Try to start PostgreSQL
        if brew services start postgresql@14 2>/dev/null || brew services start postgresql 2>/dev/null; then
            echo -e "${GREEN}✅ PostgreSQL started successfully${NC}"
            sleep 2  # Wait for PostgreSQL to fully start
        else
            echo -e "${RED}❌ Failed to start PostgreSQL${NC}"
            echo -e "${YELLOW}💡 Please install PostgreSQL: brew install postgresql${NC}"
            exit 1
        fi
    fi
}

# Function to create database user
create_user() {
    echo -e "${YELLOW}👤 Creating database user...${NC}"
    
    # Check if user already exists
    if psql -U $ADMIN_USER -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1; then
        echo -e "${YELLOW}⚠️  User '$DB_USER' already exists${NC}"
    else
        echo -e "${YELLOW}🔑 Creating user '$DB_USER'...${NC}"
        psql -U $ADMIN_USER -d postgres -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
        echo -e "${GREEN}✅ User '$DB_USER' created successfully${NC}"
    fi
    
    # Grant necessary privileges
    echo -e "${YELLOW}🔐 Granting privileges...${NC}"
    psql -U $ADMIN_USER -d postgres -c "ALTER USER $DB_USER CREATEDB;"
    psql -U $ADMIN_USER -d postgres -c "ALTER USER $DB_USER WITH SUPERUSER;"
    echo -e "${GREEN}✅ Privileges granted${NC}"
}

# Function to create database
create_database() {
    echo -e "${YELLOW}🗄️  Creating database...${NC}"
    
    # Check if database already exists
    if psql -U $ADMIN_USER -d postgres -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
        echo -e "${YELLOW}⚠️  Database '$DB_NAME' already exists${NC}"
        echo -e "${YELLOW}🔄 Dropping existing database...${NC}"
        psql -U $ADMIN_USER -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
    fi
    
    echo -e "${YELLOW}📦 Creating database '$DB_NAME'...${NC}"
    psql -U $ADMIN_USER -d postgres -c "CREATE DATABASE $DB_NAME;"
    
    echo -e "${YELLOW}🔐 Granting database privileges...${NC}"
    psql -U $ADMIN_USER -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"
    
    echo -e "${GREEN}✅ Database '$DB_NAME' created successfully${NC}"
}

# Function to test database connection
test_connection() {
    echo -e "${YELLOW}🔍 Testing database connection...${NC}"
    
    if psql -U $DB_USER -d $DB_NAME -c "SELECT version();" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Database connection successful${NC}"
        
        # Show database info
        echo -e "${YELLOW}📊 Database Information:${NC}"
        psql -U $DB_USER -d $DB_NAME -c "SELECT version();" | head -3
    else
        echo -e "${RED}❌ Database connection failed${NC}"
        exit 1
    fi
}

# Function to create environment file
create_env_file() {
    echo -e "${YELLOW}⚙️  Creating environment configuration...${NC}"
    
    ENV_FILE="backend/.env"
    
    cat > $ENV_FILE << EOF
# Database Configuration
DATABASE_URL=postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME

# Redis Configuration (optional)
REDIS_URL=redis://localhost:6379/0

# CORS Configuration
BACKEND_CORS_ORIGINS=["http://localhost:3000","http://localhost:8000","http://localhost"]
EOF
    
    echo -e "${GREEN}✅ Environment file created: $ENV_FILE${NC}"
}

# Function to initialize database schema
init_schema() {
    echo -e "${YELLOW}🏗️  Initializing database schema...${NC}"
    
    # Since we removed authentication, we don't need to create any tables
    # The application will create tables dynamically when needed
    
    echo -e "${GREEN}✅ Database schema initialized${NC}"
}

# Main execution
main() {
    echo -e "${YELLOW}📝 Configuration:${NC}"
    echo -e "  Database: $DB_NAME"
    echo -e "  User: $DB_USER"
    echo -e "  Host: $DB_HOST:$DB_PORT"
    echo -e "  Admin User: $ADMIN_USER"
    echo ""
    
    check_postgresql
    create_user
    create_database
    test_connection
    create_env_file
    init_schema
    
    echo -e "${GREEN}🎉 Database initialization completed successfully!${NC}"
    echo -e "${YELLOW}📋 Next steps:${NC}"
    echo -e "  1. cd backend && source venv/bin/activate"
    echo -e "  2. pip install -r requirements.txt"
    echo -e "  3. python main.py"
    echo ""
    echo -e "${GREEN}🌐 Application will be available at: http://localhost:8000${NC}"
}

# Run main function
main "$@"