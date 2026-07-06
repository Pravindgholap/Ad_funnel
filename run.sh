#!/bin/bash
# Convenience script to boot the mock API server locally.
# chmod +x run.sh, then ./run.sh
uvicorn mock_api.server:app --reload --port 8000