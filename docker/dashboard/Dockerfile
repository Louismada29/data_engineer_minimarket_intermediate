FROM python:3.11-slim

RUN pip install --no-cache-dir \
      fastapi==0.111.0 \
      "uvicorn[standard]==0.30.1" \
      clickhouse-connect==0.7.19

WORKDIR /app
COPY dashboard/ /app/

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
