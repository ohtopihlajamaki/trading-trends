ARG PYTHON_IMAGE=python:3.13.15-slim
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY prep.py prep_day.sql ./
ENTRYPOINT ["python", "-u", "prep.py"]
