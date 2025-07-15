# Use Python 3.10 slim image as base
FROM python:3.10-slim

# Set working directory in container
WORKDIR /app

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=17653
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create uploads directory
RUN mkdir -p uploads

# Expose port 17653
EXPOSE 17653

# Run the Flask application
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=17653", "--debug"]
