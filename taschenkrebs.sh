#!/usr/bin/env bash

# 1) Ensure we pick up your user‐installed console scripts
export PATH="/gpfs/home/bockelma/.local/bin:/gpfs/home/bockelma/opt/python3/bin:$PATH"

# 2) Ensure Python can import from your user site‐packages
export PYTHONPATH="/gpfs/home/bockelma/.local/lib/python3/site-packages:$PYTHONPATH"

# 3) Go to the script folder
cd /gpfs/work/bockelma/data/THUENEN/TASCHENKREBS

# 4) Run and log output
./taschenkrebs.py >> taschenkrebs.log 2>&1
