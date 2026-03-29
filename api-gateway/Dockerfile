FROM python:3.12-slim

WORKDIR /home/flask-app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY span_logger.py .

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "1", "--worker-class", "gthread", "--threads", "16", "--timeout", "30", "--log-level=info", "app:app"]
