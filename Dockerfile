# bookworm (glibc 2.36) satisfies the anki 26.5 wheel's glibc >= 2.35 requirement.
# Do NOT downgrade to bullseye/slim-buster — the wheel won't load there.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "worker.py"]
