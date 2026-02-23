#!/bin/bash

# ZeroGEX Ingestion Service Management Script
# Convenience wrapper for common systemctl operations

SERVICE_NAME="zerogex-oa-ingestion"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Function to print usage
print_usage() {
    echo "ZeroGEX Ingestion Service Manager"
    echo ""
    echo "Usage: ./manage_service.sh [command]"
    echo ""
    echo "Commands:"
    echo "  start      - Start the service"
    echo "  stop       - Stop the service"
    echo "  restart    - Restart the service"
    echo "  status     - Show service status"
    echo "  logs       - Show live logs (follow mode)"
    echo "  logs-tail  - Show last 100 log lines"
    echo "  enable     - Enable service to start on boot"
    echo "  disable    - Disable service from starting on boot"
    echo "  reload     - Reload configuration (graceful)"
    echo "  health     - Check service health and recent errors"
    echo ""
}

# Function to check if service exists
check_service() {
    if ! systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
        echo -e "${RED}ERROR: Service ${SERVICE_NAME} not found${NC}"
        echo "Run: sudo ./install_systemd_service.sh"
        exit 1
    fi
}

# Function to show health check
show_health() {
    echo -e "${GREEN}Service Health Check${NC}"
    echo "===================="
    
    # Service status
    if systemctl is-active --quiet "${SERVICE_NAME}"; then
        echo -e "Status: ${GREEN}ACTIVE${NC}"
    else
        echo -e "Status: ${RED}INACTIVE${NC}"
    fi
    
    # Uptime
    UPTIME=$(systemctl show "${SERVICE_NAME}" --property=ActiveEnterTimestamp --value)
    if [ -n "$UPTIME" ]; then
        echo "Started: $UPTIME"
    fi
    
    # Memory usage
    MEMORY=$(systemctl show "${SERVICE_NAME}" --property=MemoryCurrent --value)
    if [ "$MEMORY" != "[not set]" ] && [ -n "$MEMORY" ]; then
        MEMORY_MB=$((MEMORY / 1024 / 1024))
        echo "Memory: ${MEMORY_MB} MB"
    fi
    
    # Recent errors
    echo ""
    echo "Recent Errors (last 10):"
    echo "------------------------"
    journalctl -u "${SERVICE_NAME}" -p err -n 10 --no-pager || echo "No recent errors"
    
    # Recent warnings
    echo ""
    echo "Recent Warnings (last 5):"
    echo "-------------------------"
    journalctl -u "${SERVICE_NAME}" -p warning -n 5 --no-pager || echo "No recent warnings"
}

# Main command handler
if [ $# -eq 0 ]; then
    print_usage
    exit 0
fi

check_service

COMMAND=$1

case $COMMAND in
    start)
        echo -e "${GREEN}Starting ${SERVICE_NAME}...${NC}"
        sudo systemctl start "${SERVICE_NAME}"
        sleep 2
        sudo systemctl status "${SERVICE_NAME}" --no-pager
        ;;
    
    stop)
        echo -e "${YELLOW}Stopping ${SERVICE_NAME}...${NC}"
        sudo systemctl stop "${SERVICE_NAME}"
        sleep 1
        echo -e "${GREEN}Service stopped${NC}"
        ;;
    
    restart)
        echo -e "${YELLOW}Restarting ${SERVICE_NAME}...${NC}"
        sudo systemctl restart "${SERVICE_NAME}"
        sleep 2
        sudo systemctl status "${SERVICE_NAME}" --no-pager
        ;;
    
    status)
        sudo systemctl status "${SERVICE_NAME}" --no-pager -l
        ;;
    
    logs)
        echo -e "${GREEN}Showing live logs (Ctrl+C to exit)...${NC}"
        sudo journalctl -u "${SERVICE_NAME}" -f
        ;;
    
    logs-tail)
        echo -e "${GREEN}Last 100 log lines:${NC}"
        sudo journalctl -u "${SERVICE_NAME}" -n 100 --no-pager
        ;;
    
    enable)
        echo -e "${GREEN}Enabling ${SERVICE_NAME} to start on boot...${NC}"
        sudo systemctl enable "${SERVICE_NAME}"
        echo -e "${GREEN}Service enabled${NC}"
        ;;
    
    disable)
        echo -e "${YELLOW}Disabling ${SERVICE_NAME} from starting on boot...${NC}"
        sudo systemctl disable "${SERVICE_NAME}"
        echo -e "${YELLOW}Service disabled${NC}"
        ;;
    
    reload)
        echo -e "${GREEN}Reloading ${SERVICE_NAME} configuration...${NC}"
        sudo systemctl reload "${SERVICE_NAME}"
        echo -e "${GREEN}Configuration reloaded${NC}"
        ;;
    
    health)
        show_health
        ;;
    
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac
