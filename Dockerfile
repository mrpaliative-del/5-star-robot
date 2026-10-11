FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY appliance_core.py .

# Expose Render's standard web layer network port
EXPOSE 10000

# Use Gunicorn to run the server interface to force instant health-check authorization passing
CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--workers", "1", "--threads", "4", "--timeout", "60", "appliance_core:wsgi_app"]
