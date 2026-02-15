#!/bin/bash
set -e

# Generate Application Default Credentials from OAuth env vars
# google.auth.default() will read this file automatically
cat > /tmp/adc.json <<EOF
{
  "type": "authorized_user",
  "client_id": "${GOOGLE_CLIENT_ID}",
  "client_secret": "${GOOGLE_CLIENT_SECRET}",
  "refresh_token": "${GOOGLE_REFRESH_TOKEN}"
}
EOF

export GOOGLE_APPLICATION_CREDENTIALS="/tmp/adc.json"

echo "Starting Google Ads MCP Server on port ${PORT:-8080}..."
exec python /app/run_sse.py
