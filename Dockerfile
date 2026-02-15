FROM python:3.12-slim

WORKDIR /app

# Pin to a specific commit for security
# Verify latest at: https://github.com/googleads/google-ads-mcp/commits/main/
RUN pip install --no-cache-dir \
    "google-ads-mcp @ git+https://github.com/googleads/google-ads-mcp.git@85dab37"

COPY entrypoint.sh /app/entrypoint.sh
COPY run_sse.py /app/run_sse.py
RUN chmod +x /app/entrypoint.sh

ENV PORT=8080

EXPOSE ${PORT}

CMD ["/app/entrypoint.sh"]
