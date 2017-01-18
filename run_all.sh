#!/bin/bash

tmux kill-session -t rettiwt
tmux new-session -s rettiwt -n bash -d
tmux new-window -t 1
tmux send-keys -t rettiwt:1 'cd kafka' C-m
tmux send-keys -t rettiwt:1 'python twitter_api_to_kafka.py' C-m
tmux new-window -t 2
tmux send-keys -t rettiwt:2 'cd spark_stream' C-m
if [ $# -ne 1 ]; then
	tmux send-keys -t rettiwt:2 './run.sh' C-m
else
	tmux send-keys -t rettiwt:2 './run.sh '"$1"'' C-m
if
