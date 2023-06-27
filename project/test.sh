#!/bin/bash

python ./data/pipeline.py
python test.py

# Wait for user input before exiting
read -p "Press Enter to continue..."
