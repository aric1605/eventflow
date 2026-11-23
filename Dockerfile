# EventFlow Production Container
FROM node:20-alpine

# Set deployment working directory
WORKDIR /app

# Ensure proper permissions and install only runtime dependencies
COPY package*.json ./
RUN npm ci --omit=dev

# Copy application source code and static assets
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/
COPY public/ ./public/

# Run as unprivileged user for security
USER node

# Expose HTTP port (configurable at runtime via PORT environment variable)
EXPOSE 10000

# Start unified EventFlow server (runs migrations, API, worker, sweeper)
CMD ["node", "src/server.js"]
