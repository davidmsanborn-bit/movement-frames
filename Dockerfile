FROM node:20-slim
COPY --from=mwader/static-ffmpeg:7.1 /ffmpeg /usr/bin/ffmpeg
COPY --from=mwader/static-ffmpeg:7.1 /ffprobe /usr/bin/ffprobe
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["node", "index.js"]
