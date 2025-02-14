#!/bin/bash

# Tên session tmux cần xóa
SESSION_NAME="raft-network"

# Kiểm tra nếu session tồn tại, thì kill
tmux has-session -t $SESSION_NAME 2>/dev/null
if [ $? -eq 0 ]; then
    tmux kill-session -t $SESSION_NAME
    echo "Session $SESSION_NAME đã bị xóa."
else
    echo "Session $SESSION_NAME không tồn tại."
fi
