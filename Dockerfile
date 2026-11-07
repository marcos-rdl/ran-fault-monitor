FROM python:3.12-slim
ARG SERVICE
WORKDIR /app
COPY ${SERVICE}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ${SERVICE}/ .
ENV PYTHONUNBUFFERED=1
