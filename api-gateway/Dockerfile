FROM python:3.12-slim

WORKDIR /home/flask-app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "1", "--timeout", "30", "--log-level=info", "app:app"]
