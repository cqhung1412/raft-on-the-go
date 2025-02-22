# raft-on-the-go

Simple RAFT implementation using Golang.

## Useful commands

start
```sh
go run main.go
```

```sh
protoc --go_out=. --go-grpc_out=. ./proto/raft.proto
```

Quick start network
Note: Using Tmux
```sh
chmod +x ./script/start-raft-network.sh
./script/start-raft-network.sh
```

Quick kill network
```sh
chmod +x ./script/kill-raft-network.sh
./script/kill-raft-network.sh
```

create client
```sh
go run main.go --client --leader=<leader-port|5001> --term=<leader-term|1>
```

**Inspect Node information**
http-port = grpc-port + 1000
example: grpc port = 5001 -> http port = 6001
```sh
curl localhost:<http-port>/inspect
```