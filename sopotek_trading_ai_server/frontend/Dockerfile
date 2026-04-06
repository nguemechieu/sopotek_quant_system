FROM node:22-alpine AS deps
WORKDIR /app/frontend
COPY package.json package-lock.json* ./
RUN npm ci

FROM node:22-alpine AS builder
WORKDIR /app/frontend
COPY --from=deps /app/frontend/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:22-alpine AS runner
WORKDIR /app/frontend
ENV NODE_ENV=production
COPY --from=builder /app/frontend ./
EXPOSE 3000
CMD ["npm", "run", "start"]
