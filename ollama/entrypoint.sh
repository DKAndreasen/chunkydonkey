#!/bin/bash
ollama serve &
pid=$!

sleep 5
ollama pull qwen3-vl:30b-a3b-instruct
ollama create Qwen/Qwen3-VL-8B-Instruct-FP8 -f /Modelfile

wait $pid