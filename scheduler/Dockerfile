FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scheduler.py scheduling_logic.py ./

CMD ["python", "scheduler.py"]

