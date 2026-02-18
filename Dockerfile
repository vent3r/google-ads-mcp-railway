FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "google-ads-mcp @ git+https://github.com/googleads/google-ads-mcp.git@85dab37" \
    supabase

COPY entrypoint.sh /app/entrypoint.sh
COPY run_sse.py /app/run_sse.py
COPY run_server.py /app/run_server.py
COPY tools/ /app/tools/
RUN chmod +x /app/entrypoint.sh

ENV PORT=8080

EXPOSE ${PORT}

CMD ["/app/entrypoint.sh"]
