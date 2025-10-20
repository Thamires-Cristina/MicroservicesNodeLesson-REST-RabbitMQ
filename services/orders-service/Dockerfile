FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

# Gerar Prisma Client
RUN npx prisma generate

EXPOSE 3002
CMD ["node", "src/index.js"]
