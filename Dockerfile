# Build with HDoi as context: docker build HDoi
FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /opt/cham-cong/HDoi
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --chown=node:node src/ ./src/
RUN mkdir -p /data/images && chown -R node:node /data/images
USER node
EXPOSE 3100
CMD ["node", "src/app.js"]
