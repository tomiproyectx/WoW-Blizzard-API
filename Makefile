# Variables
IMAGE_NAME = wow-airflow
COMPOSE    = docker compose

.PHONY: help build env init up down ps logs test

help:
	@echo "Comandos disponibles:"
	@echo "  make build   - Construye la imagen Docker ($(IMAGE_NAME))"
	@echo "  make env     - Crea .env a partir de .env.example (si no existe)"
	@echo "  make init    - Inicializa Airflow (DB + usuario + Variables)"
	@echo "  make up      - Levanta Airflow (webserver + scheduler)"
	@echo "  make down    - Detiene y borra los servicios de docker-compose"
	@echo "  make ps      - Muestra el estado de los contenedores"
	@echo "  make logs    - Sigue los logs del webserver de Airflow"
	@echo "  make test    - Ejecuta los tests con pytest via uv"

# Construye la imagen Docker basada en el Dockerfile
build:
	docker build -t $(IMAGE_NAME):latest .

# Crea .env solo si no existe, a partir de .env.example
env:
	test -f .env || cp .env.example .env

# Inicializa Airflow: migra la DB, crea usuario admin y Variables Blizzard
init:
	$(COMPOSE) up airflow-init

# Levanta webserver + scheduler (depende de init)
up: init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler
	$(COMPOSE) ps

# Detiene y elimina los contenedores definidos en docker-compose.yml
down:
	$(COMPOSE) down -v

# Muestra el estado de los servicios
ps:
	$(COMPOSE) ps

# Sigue los logs del webserver
logs:
	$(COMPOSE) logs -f airflow-webserver

# Ejecuta los tests unitarios en el host usando uv
test:
	uv run pytest -q
