# GridPulse -- one-command operations
# On Windows, run these via Git Bash, WSL, or `make` if installed;
# every target also works copy-pasted directly into PowerShell.

.PHONY: up down producer consumer metrics test sql

up:        ## Start the full stack (Redpanda, TimescaleDB, Grafana)
	docker compose up -d

down:      ## Stop the stack (data persists in volumes)
	docker compose down

producer:  ## Stream live Elexon system prices into Redpanda
	python producers/system_prices_producer.py

producers-all:  ## Stream all sources (run each in its own terminal)
	python producers/system_prices_producer.py & \
	python producers/carbon_intensity_producer.py & \
	python producers/generation_mix_producer.py & \
	python producers/weather_producer.py & \
	wait

consumer:  ## Land system prices into TimescaleDB
	python consumers/system_prices_consumer.py

metrics:   ## Run the domain intelligence engine
	python metrics/calculator.py

sql:       ## Apply reference data and continuous aggregates
	type sql\001_reference_data.sql | docker exec -i timescaledb psql -U gridpulse -d gridpulse
	type sql\002_continuous_aggregates.sql | docker exec -i timescaledb psql -U gridpulse -d gridpulse

test:      ## Run the test suite
	python -m pytest tests/ -v
