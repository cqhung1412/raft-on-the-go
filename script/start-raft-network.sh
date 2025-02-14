#!/bin/bash

# Tên phiên tmux
SESSION_NAME="raft-network"

# Tạo phiên tmux mới và mở cửa sổ đầu tiên
tmux new-session -d -s $SESSION_NAME

# Chia cửa sổ thành 5 pane
tmux split-window -h # Chia thành 2 pane ngang
tmux split-window -v # Chia pane bên phải thành 2 pane dọc
tmux select-pane -t 0
tmux split-window -v # Chia pane bên trái thành 2 pane dọc
tmux select-pane -t 2
tmux split-window -v # Chia pane dưới bên phải thành 2 pane dọc

# Chạy lệnh "go run main.go" trong từng pane
for i in {0..4}; do
   tmux select-pane -t $i
   tmux send-keys "go run main.go" C-m
done

# Đính kèm vào phiên tmux
tmux attach-session -t $SESSION_NAME
