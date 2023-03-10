FROM python:3.10

# Сборка зависимостей
ARG BUILD_DEPS="curl"
RUN apt-get update && apt-get install -y $BUILD_DEPS

# Установка poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.4.0 POETRY_HOME=/root/poetry python3 -
ENV PATH="${PATH}:/root/poetry/bin"

# Инициализация проекта
COPY . .
WORKDIR /example_pipeline


COPY poetry.lock pyproject.toml /
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

COPY . .

CMD ["python3", "pipeline.py"]