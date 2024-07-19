# Use the official Python image
FROM python:3.12.1

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install Prisma

# Install any Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
# Copy the content of the local src directory to the working directory
COPY . /app
RUN prisma generate
EXPOSE 8000
# Run FastAPI application using uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
