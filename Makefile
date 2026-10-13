.PHONY: help build start start-prod start-dev stop clean logs status test
help:
	@echo "Targets: build, start, start-prod, start-dev, stop, clean, logs, status, test"
build:
	docker compose build
start: start-prod
start-prod:
	docker compose up -d
start-dev:
	docker compose up -d
stop:
	docker compose down
# Destructive only within this Compose project; never prune the shared host.
clean:
	@echo "This removes this project's containers and volumes. Run only for disposable local data."
	docker compose down -v --remove-orphans
logs:
	docker compose logs -f
status:
	docker compose ps
test:
	curl --fail http://localhost/health
