.PHONY: build test e2e mysql-up mysql-down
build:
	mkdir -p bin
	go build -o bin/etl ./cmd/etl
test:
	go test -race ./...
mysql-up:
	docker compose up -d --wait mysql
mysql-down:
	docker compose stop mysql
e2e:
	ETL_E2E=1 go test -tags integration ./e2e -count=1 -v
