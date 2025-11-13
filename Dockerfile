FROM --platform=linux/amd64 ghcr.io/astral-sh/uv:0.8.13-python3.13-bookworm-slim

ENV PYTHONPYCACHEPREFIX=/usr/src/pycache \
    PYTHONPATH=/app \
    TZ=Asia/Tokyo
WORKDIR /app

RUN uv venv && \
    echo "source .venv/bin/activate" > ~/.bashrc

COPY ./core_analytics ./core_analytics
COPY ./config ./config
COPY app.py ./

COPY pyproject.toml ./
COPY uv.lock ./
RUN uv sync --frozen --verbose && uv cache clean

EXPOSE 8080
CMD ["uv", "run", "gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "1", "app:app", "--bind=0.0.0.0:8080"]
