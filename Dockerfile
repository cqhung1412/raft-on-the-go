FROM golang:1.23-alpine

WORKDIR /app

# Install tools needed for development
RUN apk add --no-cache iptables iputils bash curl

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate protobuf files (if needed)
# RUN protoc --go_out=. --go-grpc_out=. ./proto/raft.proto

# Expose ports for GRPC (5000-5005) and HTTP (6000-6005)
EXPOSE 5000-5005
EXPOSE 6000-6005

# Default command to run when container starts
CMD ["go", "run", "main.go"]