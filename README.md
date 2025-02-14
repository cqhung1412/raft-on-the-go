# raft-on-the-go

Simple RAFT implementation using Golang.

## Useful commands

start
```cmd
go run main.go
```

```cmd
protoc --go_out=. --go-grpc_out=. ./proto/raft.proto
```

Quick start network
Note: Using Tmux
```cmd
chmod +x ./script/start-raft-network.sh
./script/start-raft-network.sh
```

Quick kill network
```cmd
chmod +x ./script/kill-raft-network.sh
./script/kill-raft-network.sh
```

create client
```cmd
go run main.go --client --leader=<leader-port|5001> --term=<leader-term|1>
```
