FROM python:3.11.1-alpine3.17
LABEL maintainer="hud.net.ru"

ENV PYTHONUNBUFFERED 1

WORKDIR /app

ARG DEV=false

EXPOSE 8000

COPY . .

ENV PYTHONPATH=/app

RUN python -m venv /py && \
  /py/bin/pip install --upgrade pip && \
  apk add --update --no-cache postgresql-client && \
  apk add --update --no-cache --virtual .tmp-build-deps \
  gcc \
  g++ \
  build-base \
  postgresql-dev \
  musl-dev \
  python3-dev \
  zlib \
  zlib-dev \
  linux-headers \
  libc-dev \
  openssl-dev \
  freetype-dev \
  libpng-dev \
  libffi-dev && \
  apk add --no-cache libstdc++ openblas-dev hdf5-dev && \
  /py/bin/pip install -r requirements.txt && \
  if [ $DEV = "true" ]; \
  then /py/bin/pip install -r requirements-dev.txt ; \
  fi && \
  apk del .tmp-build-deps && \
  mkdir tmp

RUN ["chmod", "+x", "/app/run.sh"]

ENV PATH="/app:/py/bin:$PATH"

ENV NAME FastAPI_Klines_Service

CMD ["run.sh"]