#!/bin/bash

python ./data/pipeline_cycles.py
python test.py

# Wait for user input before exiting
read -p "Press Enter to continue..."