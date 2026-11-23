FROM python:3.13-slim

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway_logs_to_s3.py .

CMD ["python", "gateway_logs_to_s3.py"]