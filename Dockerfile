#python slim installation
FROM python:3.10-slim

#  dependencies for ODBC
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# ODBC Driver 18 for SQL Server
RUN curl -fSsL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl -fSsL https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && DEBIAN_FRONTEND=noninteractive ACCEPT_EULA=Y apt-get install -y \
    msodbcsql18

# Set working directory
WORKDIR /app

# Copy script and requirements
COPY main.py /app/
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

#run
CMD ["python", "main.py"]

