FROM python:3.11-slim

WORKDIR /app

# Встановлюємо залежності Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо вихідний код і папки зі схемами
COPY sync_service.py .
COPY schemas/ ./schemas/

CMD ["python", "-u", "sync_service.py"]