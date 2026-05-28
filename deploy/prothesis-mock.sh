#!/bin/bash

while true
do
    echo "hello"

    if IFS= read -r -t 0.01 line; then
        echo "stdin: $line"
    fi

    sleep 2
done