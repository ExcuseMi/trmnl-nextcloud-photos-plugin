FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["hypercorn", "--bind", "0.0.0.0:8080", "--workers", "3", "app:app"]
