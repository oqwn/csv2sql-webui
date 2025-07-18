#!/bin/bash

# SQL WebUI Database Cleanup Script
# This script removes the database and user created for the SQL WebUI application

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DB_NAME="sqlwebui"
DB_USER="postgres"
ADMIN_USER=$(whoami)

echo -e "${YELLOW}🧹 Starting SQL WebUI Database Cleanup${NC}"

# Function to check if PostgreSQL is running
check_postgresql() {
    echo -e "${YELLOW}📋 Checking PostgreSQL status...${NC}"
    
    if brew services list | grep -q "postgresql.*started"; then
        echo -e "${GREEN}✅ PostgreSQL is running${NC}"
        return 0
    else
        echo -e "${RED}❌ PostgreSQL is not running${NC}"
        echo -e "${YELLOW}Please start PostgreSQL first${NC}"
        exit 1
    fi
}

# Function to drop database
drop_database() {
    echo -e "${YELLOW}🗑️  Dropping database...${NC}"
    
    # Check if database exists
    if psql -U $ADMIN_USER -d postgres -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
        echo -e "${YELLOW}🔄 Dropping database '$DB_NAME'...${NC}"
        psql -U $ADMIN_USER -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
        echo -e "${GREEN}✅ Database '$DB_NAME' dropped successfully${NC}"
    else
        echo -e "${YELLOW}⚠️  Database '$DB_NAME' does not exist${NC}"
    fi
}

# Function to drop user
drop_user() {
    echo -e "${YELLOW}👤 Dropping database user...${NC}"
    
    # Check if user exists
    if psql -U $ADMIN_USER -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1; then
        echo -e "${YELLOW}🔄 Dropping user '$DB_USER'...${NC}"
        psql -U $ADMIN_USER -d postgres -c "DROP USER IF EXISTS $DB_USER;"
        echo -e "${GREEN}✅ User '$DB_USER' dropped successfully${NC}"
    else
        echo -e "${YELLOW}⚠️  User '$DB_USER' does not exist${NC}"
    fi
}

# Function to remove environment file
remove_env_file() {
    echo -e "${YELLOW}🗑️  Removing environment file...${NC}"
    
    ENV_FILE="backend/.env"
    
    if [ -f "$ENV_FILE" ]; then
        rm "$ENV_FILE"
        echo -e "${GREEN}✅ Environment file removed${NC}"
    else
        echo -e "${YELLOW}⚠️  Environment file does not exist${NC}"
    fi
}

# Main execution
main() {
    echo -e "${YELLOW}📝 Cleanup Configuration:${NC}"
    echo -e "  Database: $DB_NAME"
    echo -e "  User: $DB_USER"
    echo -e "  Admin User: $ADMIN_USER"
    echo ""
    
    echo -e "${RED}⚠️  This will permanently delete the database and user!${NC}"
    read -p "Are you sure you want to continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
        exit 0
    fi
    
    check_postgresql
    drop_database
    drop_user
    remove_env_file
    
    echo -e "${GREEN}🎉 Database cleanup completed successfully!${NC}"
}

# Run main function
main "$@"