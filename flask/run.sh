#!/bin/bash

tmux kill-session -t webserver
tmux new-session -s webserver -n bash -d
tmux new-window -t 1
tmux send-keys -t webserver:1 'sudo python run.py' C-m
