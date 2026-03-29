FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY updater/update_data.py .

CMD ["python", "update_data.py"]
