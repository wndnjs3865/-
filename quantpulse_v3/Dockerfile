FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[all]" 2>/dev/null || pip install --no-cache-dir pydantic aiofiles psutil python-dotenv fastapi uvicorn

COPY . .

# Create data dir
RUN mkdir -p data

EXPOSE 8000

CMD ["python", "main.py"]
