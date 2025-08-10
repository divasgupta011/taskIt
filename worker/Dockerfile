FROM golang:1.22-alpine

WORKDIR /app

COPY . .

RUN go mod init task-worker && go get github.com/redis/go-redis/v9 && go build -o worker .

CMD ["./worker"]
