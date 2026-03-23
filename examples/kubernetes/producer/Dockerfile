FROM python:3.7.1-alpine3.7

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

RUN mkdir -p /app && chown appuser:appgroup /app
WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY --chown=appuser:appgroup . .

USER appuser

CMD ["python", "producer.py"]
