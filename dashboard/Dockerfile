FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Install serve globally
RUN npm install -g serve

# Copy source code
COPY . .

# Build the app
RUN npm run build

# Expose port
EXPOSE 3000

# Serve the build folder
CMD ["serve", "-s", "build", "-l", "3000"]