#!/bin/bash
set -e

case "$1" in
    "start")
        docker-compose build
        docker-compose up -d
        docker-compose exec seeder python3 /seeder.py setup
        ;;
    "stop")
        docker-compose down -v --remove-orphans
        ;;
    "seed")
        docker-compose exec seeder python3 /seeder.py seed --start "$2" --end "$3" --count "$4"
        ;;
    "check")
        docker-compose exec seeder python3 /seeder.py check
        ;;
    *)
        echo "Usage:"
        echo "  ./run.sh start                    - Start services"
        echo "  ./run.sh stop                     - Stop services"
        echo "  ./run.sh seed START END COUNT     - Generate data"
        echo "  ./run.sh check                    - Check data"
        echo "  ./run.sh iex                      - Start IEx console"
        echo ""
        echo "Example:"
        echo "  ./run.sh start"
        echo "  ./run.sh seed 2024-01 2024-11 1000"
        echo "  ./run.sh check"
        ;;
esac