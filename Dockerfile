FROM python:3.12-slim

WORKDIR /app

COPY data_runner/requirements.txt data_runner/requirements.txt
RUN pip install --no-cache-dir -r data_runner/requirements.txt

COPY . .

CMD ["python", "main.py"]
