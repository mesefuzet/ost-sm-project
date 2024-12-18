FROM alpine:latest

# Install SQLite
RUN apk add --no-cache sqlite

# Copy the database file to the container
COPY hai-dataset.db /data/data.db

# Set working directory
WORKDIR /data

# Command to keep the container running
CMD ["sqlite3", "data.db"]
