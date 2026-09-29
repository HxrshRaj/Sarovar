FROM python:3.12-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY airflow/include /opt/sarovar/lib
COPY ai/sarovar_ai /app/ai/sarovar_ai
COPY docs/catalog.yml /opt/sarovar/catalog.yml
COPY api/app /app/app
ENV PYTHONPATH=/app:/opt/sarovar/lib CATALOG_PATH=/opt/sarovar/catalog.yml
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
