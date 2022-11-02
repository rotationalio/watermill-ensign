
ci: tools lint test

tools:
	 go install honnef.co/go/tools/cmd/staticcheck@latest
	 go install golang.org/x/tools/cmd/goimports@latest

lint:
	go vet ./...
	staticcheck ./...

up:
	docker-compose up

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -parallel 30 -timeout=15m ./...

bench:
	go test -bench ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

.PHONY: ci tools lint up test test_v test_short test_race test_stress bench fmt update_watermill