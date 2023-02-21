build:
	echo "Compiling for target Platforms"
	GOOS=linux GOARCH=386 go build -o bin/goktm-linux-386 cmd/main/main.go
	GOOS=darwin GOARCH=amd64 go build -o bin/goktm cmd/main/main.go

run:
	go run cmd/main/main.go
