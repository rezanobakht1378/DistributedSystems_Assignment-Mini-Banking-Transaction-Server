FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py models.py requestForms.py gateway.py self_healing_controller.py shard_map.py shard-map.json ./

EXPOSE 8000

CMD ["python", "main.py"]