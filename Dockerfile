FROM python:3.14-slim-bookworm AS builder

ENV HOME_DIR="/app" \
    POETRY_VERSION=2.4.1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_NO_INTERACTION=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR $HOME_DIR

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-ansi && rm -rf "$POETRY_CACHE_DIR"

FROM python:3.14-slim-bookworm

ENV HOME_DIR="/app" \
    APP_DIR="/app/src" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    CONFIG_FILE="config.docker.yml"

RUN groupadd -r fastapi && useradd -r -g fastapi fastapi

WORKDIR $HOME_DIR

COPY --from=builder $HOME_DIR/.venv $HOME_DIR/.venv
COPY --chown=fastapi:fastapi . .

USER fastapi

CMD ["uvicorn", "src.main:app", "--workers", "2", "--port", "8080", "--host", "0.0.0.0"]
