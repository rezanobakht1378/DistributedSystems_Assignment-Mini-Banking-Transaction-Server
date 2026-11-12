FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py models.py requestForms.py gateway.py self_healing_controller.py db_controller.py shard_map.py shard-map.json ./

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin bank
USER 10001

EXPOSE 8000

CMD ["python", "main.py"]
