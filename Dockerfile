FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install Python deps
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

# Copy source + schema
COPY src ./src
COPY schema.sql ./schema.sql

EXPOSE 5000

CMD ["uvicorn", "src.chunkydonkey.main:app", "--host", "0.0.0.0", "--port", "5000"]
