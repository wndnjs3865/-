FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Application code
COPY . .
RUN mkdir -p data

# Non-root user for security
RUN useradd -m -r quantpulse && chown -R quantpulse:quantpulse /app
USER quantpulse

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os,sys; sys.exit(0 if os.path.exists('data/audit.db') else 1)"

CMD ["python", "main.py"]
