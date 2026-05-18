/**
 * MOTIQ — frame extraction service (Railway / Node)
 * Universal motion-based strategy for all movement types.
 */

const express = require("express");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { Readable } = require("stream");
const { pipeline } = require("stream/promises");
const { randomUUID } = require("crypto");
const { execFile, spawn } = require("child_process");
const { promisify } = require("util");
const { createClient } = require("@supabase/supabase-js");

const execFileAsync = promisify(execFile);

const FFMPEG_PATH = process.env.FFMPEG_PATH || "/usr/bin/ffmpeg";
const FFPROBE = process.env.FFPROBE_PATH || "/usr/bin/ffprobe";
const PORT = Number(process.env.PORT) || 3000;

const FIXED_FALLBACK = {
  shooting: [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
  deadlift: [1, 2, 3, 4, 5, 6, 7, 8],
  bench: [1, 2, 3, 4, 5, 6, 7, 8],
  ohp: [1, 2, 3, 4, 5, 6, 7, 8],
  lunge: [1, 2, 3, 4, 5, 6, 7, 8],
  rdl: [1, 2, 3, 4, 5, 6, 7, 8],
  default: [1, 2, 3],
};

const UNIFORM_SEGMENT_COUNT = 10;
/** If coefficient of variation of JPEG file sizes is below this, treat frames as too similar. */
const LOW_VARIANCE_MAX_CV = 0.035;
const GAME_CROPS_MAX_REQUEST = 100;
const GAME_CROPS_SIGNED_URL_TTL_SEC = 60 * 60;
const GAME_CROPS_MIN_EDGE_PX = 32;
const GAME_CROPS_JPEG_QUALITY = 85;
const CENTER_CROP_WIDTH_RATIO = 0.08;
const CENTER_CROP_HEIGHT_RATIO = 0.32;
const CENTER_CROP_MIN_WIDTH_PX = 96;
const CENTER_CROP_MAX_WIDTH_PX = 220;
const CENTER_CROP_MIN_HEIGHT_PX = 220;
const CENTER_CROP_MAX_HEIGHT_PX = 460;
const CENTER_CROP_UPWARD_BIAS_RATIO = 0.05;
const BBOX_EXPAND_X_PER_SIDE_RATIO = 0.08;
const BBOX_EXPAND_TOP_RATIO = 0.06;
const BBOX_EXPAND_BOTTOM_RATIO = 0.04;
const BBOX_JERSEY_LEFT_RATIO = 0.15;
const BBOX_JERSEY_RIGHT_RATIO = 0.85;
const BBOX_JERSEY_TOP_HEIGHT_RATIO = 0.55;
const CENTER_JERSEY_LEFT_RATIO = 0.14;
const CENTER_JERSEY_RIGHT_RATIO = 0.86;
const CENTER_JERSEY_TOP_HEIGHT_RATIO = 0.58;

function parseEnvPositiveInt(raw, fallback, min, max) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  const t = Math.trunc(n);
  if (t < min || t > max) return fallback;
  return t;
}

/**
 * In-process gate for heavy CV routes (shared Railway replica, memory-capped).
 *
 * FRAMES_MAX_CONCURRENCY default 1: one ~99-crop game-film job (~60–120s wall
 * with download + ffmpeg) can OOM the replica; overlapping runs stack disk +
 * ffmpeg + response buffers.
 *
 * FRAMES_QUEUE_WAIT_MS default 120000 (2 min): game-film callers use
 * AbortSignal.timeout(240_000). One job ahead (~90–120s) + 120s queue wait +
 * own run stays under 240s; longer backlog fails fast with 503 retryable.
 */
const FRAMES_MAX_CONCURRENCY = parseEnvPositiveInt(
  process.env.FRAMES_MAX_CONCURRENCY,
  1,
  1,
  8,
);
const FRAMES_QUEUE_WAIT_MS = parseEnvPositiveInt(
  process.env.FRAMES_QUEUE_WAIT_MS,
  120_000,
  1_000,
  600_000,
);

function createHeavyGate(maxConcurrency, maxQueueWaitMs) {
  let inFlight = 0;
  const queue = [];

  function logConcurrency(event, extra) {
    console.log(
      JSON.stringify({
        type: "frames_concurrency",
        event,
        in_flight: inFlight,
        queued: queue.length,
        max_concurrency: maxConcurrency,
        max_queue_wait_ms: maxQueueWaitMs,
        ...extra,
      }),
    );
  }

  async function acquire(meta) {
    const t0 = Date.now();
    if (inFlight < maxConcurrency) {
      inFlight += 1;
      logConcurrency("acquire_immediate", { ...meta, wait_ms: 0 });
      return 0;
    }

    logConcurrency("enqueue", meta);

    await new Promise((resolve, reject) => {
      const entry = {
        resolve,
        reject,
        meta,
        enqueuedAt: Date.now(),
        timer: null,
      };
      entry.timer = setTimeout(() => {
        const idx = queue.indexOf(entry);
        if (idx < 0) return;
        queue.splice(idx, 1);
        logConcurrency("reject_queue_timeout", {
          ...meta,
          wait_ms: Date.now() - entry.enqueuedAt,
        });
        const err = new Error("Heavy frame queue wait exceeded");
        err.code = "FRAMES_QUEUE_TIMEOUT";
        reject(err);
      }, maxQueueWaitMs);
      queue.push(entry);
    });

    const waitMs = Date.now() - t0;
    logConcurrency("dequeue", { ...meta, wait_ms: waitMs });
    return waitMs;
  }

  function release(meta) {
    inFlight = Math.max(0, inFlight - 1);
    logConcurrency("release", meta);
    while (queue.length > 0 && inFlight < maxConcurrency) {
      const next = queue.shift();
      clearTimeout(next.timer);
      inFlight += 1;
      logConcurrency("dequeue_grant", {
        ...next.meta,
        wait_ms: Date.now() - next.enqueuedAt,
      });
      next.resolve();
    }
  }

  return { acquire, release };
}

const heavyGate = createHeavyGate(FRAMES_MAX_CONCURRENCY, FRAMES_QUEUE_WAIT_MS);

function wrapHeavyHandler(handler) {
  return async (req, res) => {
    const meta = {
      path: req.path,
      request_id:
        (typeof req.headers["x-request-id"] === "string" &&
          req.headers["x-request-id"].trim()) ||
        randomUUID(),
    };
    try {
      await heavyGate.acquire(meta);
    } catch (err) {
      if (err && err.code === "FRAMES_QUEUE_TIMEOUT") {
        return res.status(503).json({
          error:
            "Frame service is busy processing other videos. Please retry shortly.",
          retryable: true,
        });
      }
      throw err;
    }
    try {
      await handler(req, res);
    } finally {
      heavyGate.release(meta);
    }
  };
}

function isWebm(filePath) {
  return path.extname(filePath).toLowerCase() === ".webm";
}

function ffmpegInputArgs(inputPath) {
  const args = ["-y", "-hide_banner", "-loglevel", "error"];
  if (isWebm(inputPath)) {
    args.push("-fflags", "+genpts");
  }
  args.push("-i", inputPath);
  return args;
}

function parseRateToFps(rate) {
  if (typeof rate !== "string" || !rate.trim()) return 0;
  const parts = rate.split("/");
  if (parts.length === 2) {
    const num = Number(parts[0]);
    const den = Number(parts[1]);
    if (Number.isFinite(num) && Number.isFinite(den) && den > 0) {
      return num / den;
    }
  }
  const parsed = Number(rate);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function ffmpegQScaleFromJpegQuality(quality) {
  const q = clamp(Math.round(quality), 1, 100);
  const mapped = Math.round(((100 - q) / 99) * 29 + 2);
  return String(clamp(mapped, 2, 31));
}

function sanitizeTmpId(value) {
  return String(value || "")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 128);
}

async function getVideoMetadata(inputPath) {
  const { stdout } = await execFileAsync(
    FFPROBE,
    [
      "-v",
      "error",
      "-print_format",
      "json",
      "-show_streams",
      "-show_format",
      inputPath,
    ],
    { maxBuffer: 20 * 1024 * 1024 },
  );
  const parsed = JSON.parse(String(stdout || "{}"));
  const streams = Array.isArray(parsed.streams) ? parsed.streams : [];
  const videoStream = streams.find((s) => s && s.codec_type === "video") || {};
  const width = Number(videoStream.width);
  const height = Number(videoStream.height);
  const fps =
    parseRateToFps(videoStream.avg_frame_rate) ||
    parseRateToFps(videoStream.r_frame_rate) ||
    0;
  const durationFormat = Number(parsed?.format?.duration);
  const durationStream = Number(videoStream.duration);
  const durationSec =
    (Number.isFinite(durationFormat) && durationFormat > 0
      ? durationFormat
      : Number.isFinite(durationStream) && durationStream > 0
        ? durationStream
        : 0);
  if (!(width > 0 && height > 0)) {
    throw new Error("ffprobe failed to resolve video width/height");
  }
  return { width, height, fps, durationSec };
}

function parseJpegDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 4) return null;
  if (buffer[0] !== 0xff || buffer[1] !== 0xd8) return null;
  let i = 2;
  while (i + 9 < buffer.length) {
    if (buffer[i] !== 0xff) {
      i += 1;
      continue;
    }
    const marker = buffer[i + 1];
    if (marker === 0xd9 || marker === 0xda) break;
    const length = buffer.readUInt16BE(i + 2);
    const isSof =
      marker >= 0xc0 &&
      marker <= 0xcf &&
      marker !== 0xc4 &&
      marker !== 0xc8 &&
      marker !== 0xcc;
    if (isSof) {
      if (i + 8 >= buffer.length) break;
      const height = buffer.readUInt16BE(i + 5);
      const width = buffer.readUInt16BE(i + 7);
      if (width > 0 && height > 0) {
        return { width, height };
      }
      break;
    }
    if (!(length > 2)) break;
    i += 2 + length;
  }
  return null;
}

function buildBboxExactWindow(rawBbox, videoMeta) {
  const [x1Raw, y1Raw, x2Raw, y2Raw] = rawBbox;
  if (
    ![x1Raw, y1Raw, x2Raw, y2Raw].every(isFiniteNumber) ||
    x2Raw <= x1Raw ||
    y2Raw <= y1Raw
  ) {
    return null;
  }
  const x1 = clamp(Math.floor(x1Raw), 0, videoMeta.width);
  const x2 = clamp(Math.ceil(x2Raw), 0, videoMeta.width);
  const y1 = clamp(Math.floor(y1Raw), 0, videoMeta.height);
  const y2 = clamp(Math.ceil(y2Raw), 0, videoMeta.height);
  if (x2 <= x1 || y2 <= y1) return null;

  const w = x2 - x1;
  const h = y2 - y1;
  const newX1 = clamp(
    Math.floor(x1 - BBOX_EXPAND_X_PER_SIDE_RATIO * w),
    0,
    videoMeta.width,
  );
  const newX2 = clamp(
    Math.ceil(x2 + BBOX_EXPAND_X_PER_SIDE_RATIO * w),
    0,
    videoMeta.width,
  );
  const newY1 = clamp(
    Math.floor(y1 - BBOX_EXPAND_TOP_RATIO * h),
    0,
    videoMeta.height,
  );
  const newY2 = clamp(
    Math.ceil(y2 + BBOX_EXPAND_BOTTOM_RATIO * h),
    0,
    videoMeta.height,
  );
  if (newX2 <= newX1 || newY2 <= newY1) return null;

  const expandedW = newX2 - newX1;
  const expandedH = newY2 - newY1;
  const jerseyX1 = clamp(
    newX1 + Math.floor(expandedW * BBOX_JERSEY_LEFT_RATIO),
    0,
    videoMeta.width,
  );
  const jerseyX2 = clamp(
    newX1 + Math.floor(expandedW * BBOX_JERSEY_RIGHT_RATIO),
    0,
    videoMeta.width,
  );
  const jerseyY1 = newY1;
  const jerseyY2 = clamp(
    newY1 + Math.floor(expandedH * BBOX_JERSEY_TOP_HEIGHT_RATIO),
    0,
    videoMeta.height,
  );
  if (jerseyX2 <= jerseyX1 || jerseyY2 <= jerseyY1) return null;
  return {
    x1: jerseyX1,
    y1: jerseyY1,
    x2: jerseyX2,
    y2: jerseyY2,
    method: "bbox_exact",
  };
}

function buildCenterHeuristicWindow(centerX, centerY, videoMeta) {
  if (!isFiniteNumber(centerX) || !isFiniteNumber(centerY)) return null;
  const cxPx = centerX * videoMeta.width;
  const cyPx = (centerY - CENTER_CROP_UPWARD_BIAS_RATIO) * videoMeta.height;
  const cropW = clamp(
    Math.floor(CENTER_CROP_WIDTH_RATIO * videoMeta.width),
    CENTER_CROP_MIN_WIDTH_PX,
    CENTER_CROP_MAX_WIDTH_PX,
  );
  const cropH = clamp(
    Math.floor(CENTER_CROP_HEIGHT_RATIO * videoMeta.height),
    CENTER_CROP_MIN_HEIGHT_PX,
    CENTER_CROP_MAX_HEIGHT_PX,
  );
  const x1 = clamp(Math.floor(cxPx - cropW / 2), 0, videoMeta.width);
  const x2 = clamp(Math.floor(cxPx + cropW / 2), 0, videoMeta.width);
  const y1 = clamp(Math.floor(cyPx - cropH / 2), 0, videoMeta.height);
  const y2 = clamp(Math.floor(cyPx + cropH / 2), 0, videoMeta.height);
  if (x2 <= x1 || y2 <= y1) return null;

  const jw = x2 - x1;
  const jh = y2 - y1;
  const jerseyX1 = clamp(
    x1 + Math.floor(jw * CENTER_JERSEY_LEFT_RATIO),
    0,
    videoMeta.width,
  );
  const jerseyX2 = clamp(
    x1 + Math.floor(jw * CENTER_JERSEY_RIGHT_RATIO),
    0,
    videoMeta.width,
  );
  const jerseyY1 = y1;
  const jerseyY2 = clamp(
    y1 + Math.floor(jh * CENTER_JERSEY_TOP_HEIGHT_RATIO),
    0,
    videoMeta.height,
  );
  if (jerseyX2 <= jerseyX1 || jerseyY2 <= jerseyY1) return null;
  return {
    x1: jerseyX1,
    y1: jerseyY1,
    x2: jerseyX2,
    y2: jerseyY2,
    method: "center_heuristic",
  };
}

function hasValidBbox(rawBbox) {
  return (
    Array.isArray(rawBbox) &&
    rawBbox.length === 4 &&
    rawBbox.every((v) => typeof v === "number" && Number.isFinite(v))
  );
}

async function runFfmpegCropToBuffer(params) {
  const { inputPath, timestamp, window } = params;
  const cropW = window.x2 - window.x1;
  const cropH = window.y2 - window.y1;
  if (!(cropW > 0 && cropH > 0)) {
    throw new Error("Invalid crop dimensions");
  }
  const vf = `crop=${cropW}:${cropH}:${window.x1}:${window.y1}`;
  const qScale = ffmpegQScaleFromJpegQuality(GAME_CROPS_JPEG_QUALITY);
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(Math.max(0, Number(timestamp) || 0)),
  ];
  if (isWebm(inputPath)) {
    args.push("-fflags", "+genpts");
  }
  args.push(
    "-i",
    inputPath,
    "-an",
    "-vf",
    vf,
    "-frames:v",
    "1",
    "-q:v",
    qScale,
    "-f",
    "image2pipe",
    "-vcodec",
    "mjpeg",
    "pipe:1",
  );

  const startedAt = Date.now();
  const outChunks = [];
  const errChunks = [];
  await new Promise((resolve, reject) => {
    const proc = spawn(FFMPEG_PATH, args, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    proc.stdout.on("data", (chunk) => outChunks.push(chunk));
    proc.stderr.on("data", (chunk) => errChunks.push(chunk));
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code === 0) return resolve();
      const errText = Buffer.concat(errChunks).toString("utf8").trim();
      reject(new Error(`ffmpeg exited ${code}: ${errText.slice(0, 500)}`));
    });
  });
  const elapsedMs = Date.now() - startedAt;
  return { buffer: Buffer.concat(outChunks), elapsedMs };
}

async function streamDownloadGameVideoToTemp(storagePath, gameId, requestId) {
  const supabase = getSupabase();
  const { data, error } = await supabase.storage
    .from("game-videos")
    .createSignedUrl(storagePath, GAME_CROPS_SIGNED_URL_TTL_SEC);
  if (error || !data?.signedUrl) {
    throw new Error(error?.message || "Failed to create signed URL for game video");
  }

  const ext = path.extname(storagePath) || ".mp4";
  const tmpName = `${sanitizeTmpId(gameId)}-${sanitizeTmpId(requestId)}${ext}`;
  const inputPath = path.join(os.tmpdir(), tmpName);
  const response = await fetch(data.signedUrl);
  if (!response.ok || !response.body) {
    throw new Error(
      `Failed to download game video (HTTP ${response.status})`,
    );
  }
  await pipeline(
    Readable.fromWeb(response.body),
    fs.createWriteStream(inputPath, { flags: "w" }),
  );
  return inputPath;
}

async function getDurationSeconds(inputPath) {
  const { stdout } = await execFileAsync(FFPROBE, [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    inputPath,
  ]);
  const d = parseFloat(String(stdout).trim());
  return Number.isFinite(d) && d > 0 ? d : null;
}

async function normalizeVideo(inputPath, analysisId) {
  const normalizedPath = path.join(
    os.tmpdir(),
    `normalized_${analysisId}.mp4`,
  );

  try {
    await execFileAsync(
      FFMPEG_PATH,
      [
        "-y",
        "-i",
        inputPath,
        "-vf",
        "fps=30,scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-crf",
        "23",
        "-c:a",
        "copy",
        "-movflags",
        "+faststart",
        normalizedPath,
      ],
      { maxBuffer: 50 * 1024 * 1024, timeout: 60000 },
    );

    console.log("[frames] normalized video:", {
      original: path.basename(inputPath),
      isScreenRecording: inputPath.toLowerCase().includes("screen"),
      normalizedPath,
    });

    return normalizedPath;
  } catch (err) {
    console.log(
      "[frames] normalization failed, using original:",
      err.message,
    );
    return inputPath;
  }
}

function isScreenRecordingPath(inputPath) {
  return (
    inputPath.includes("Screen") ||
    inputPath.includes("screen")
  );
}

/**
 * Heuristic: very similar JPEG byte sizes → scene detector likely grabbed near-duplicates
 * (e.g. screen recordings or flat lighting).
 */
function jpegPathsHaveLowSizeVariance(jpegPaths, maxCv = LOW_VARIANCE_MAX_CV) {
  if (jpegPaths.length < 4) return false;
  const sizes = jpegPaths.map((p) => fs.statSync(p).size);
  const mean = sizes.reduce((a, b) => a + b, 0) / sizes.length;
  if (mean < 1) return true;
  const variance =
    sizes.reduce((s, x) => s + (x - mean) ** 2, 0) / sizes.length;
  const std = Math.sqrt(variance);
  const cv = std / mean;
  return cv < maxCv;
}

function needsSceneFallback(jpegPaths) {
  if (jpegPaths.length < 4) return true;
  return jpegPathsHaveLowSizeVariance(jpegPaths);
}

/**
 * Run scene-change detection; writes scene_${analysisId}_NNN.jpg into outputDir.
 * @param {null | { ss: number, t: number }} windowOpts - If set, adds -ss and -t before -i (seconds).
 */
async function runSceneDetection(
  inputPath,
  outputDir,
  analysisId,
  threshold,
  windowOpts,
) {
  const pattern = path.join(outputDir, `scene_${analysisId}_%03d.jpg`);
  const vf = `select=gt(scene\\,${threshold}),scale=800:-1,fps=30`;
  const args = ["-y", "-hide_banner", "-loglevel", "error"];
  if (
    windowOpts &&
    typeof windowOpts.ss === "number" &&
    typeof windowOpts.t === "number" &&
    windowOpts.t > 0
  ) {
    args.push("-ss", String(windowOpts.ss), "-t", String(windowOpts.t));
  }
  if (isWebm(inputPath)) {
    args.push("-fflags", "+genpts");
  }
  args.push(
    "-i",
    inputPath,
    "-an",
    "-vf",
    vf,
    "-vsync",
    "vfr",
    "-pix_fmt",
    "yuvj420p",
    "-q:v",
    "2",
    pattern,
  );
  await execFileAsync(FFMPEG_PATH, args, { maxBuffer: 10 * 1024 * 1024 });
}

/**
 * Motion-oriented pass: scene filter at 0.08 + setpts reset (works better than plain scene-only
 * when cuts are subtle). Complements primary scene detection for screen / fast lifts / flat light.
 * @param {null | { ss: number, t: number }} windowOpts
 */
async function runMotionDetection(
  inputPath,
  outputDir,
  analysisId,
  windowOpts,
) {
  const pattern = path.join(outputDir, `motion_${analysisId}_%03d.jpg`);
  const vf =
    "select=gt(scene\\,0.08),setpts=N/FRAME_RATE/TB,scale=800:-1";
  const args = ["-y", "-hide_banner", "-loglevel", "error"];
  if (
    windowOpts &&
    typeof windowOpts.ss === "number" &&
    typeof windowOpts.t === "number" &&
    windowOpts.t > 0
  ) {
    args.push("-ss", String(windowOpts.ss), "-t", String(windowOpts.t));
  }
  if (isWebm(inputPath)) {
    args.push("-fflags", "+genpts");
  }
  args.push(
    "-i",
    inputPath,
    "-an",
    "-vf",
    vf,
    "-vsync",
    "vfr",
    "-pix_fmt",
    "yuvj420p",
    "-q:v",
    "2",
    pattern,
  );
  await execFileAsync(FFMPEG_PATH, args, { maxBuffer: 10 * 1024 * 1024 });
}

function listSceneFrames(outputDir, analysisId) {
  const prefix = `scene_${analysisId}_`;
  let files = [];
  try {
    files = fs.readdirSync(outputDir);
  } catch {
    return [];
  }
  return files
    .filter((f) => f.startsWith(prefix) && f.endsWith(".jpg"))
    .sort()
    .map((f) => path.join(outputDir, f));
}

function listMotionFrames(outputDir, analysisId) {
  const prefix = `motion_${analysisId}_`;
  let files = [];
  try {
    files = fs.readdirSync(outputDir);
  } catch {
    return [];
  }
  return files
    .filter((f) => f.startsWith(prefix) && f.endsWith(".jpg"))
    .sort()
    .map((f) => path.join(outputDir, f));
}

/**
 * If more than maxKeep frames, keep maxKeep evenly sampled (by index order ≈ time order).
 */
function subsampleEvenly(paths, maxKeep) {
  if (paths.length <= maxKeep) return paths;
  const n = paths.length;
  const out = [];
  for (let k = 0; k < maxKeep; k++) {
    const idx = Math.round((k * (n - 1)) / (maxKeep - 1 || 1));
    out.push(paths[idx]);
  }
  return out;
}

async function extractSingleFrameAtTime(inputPath, outputJpg, timeSeconds) {
  const args = [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(timeSeconds),
  ];
  if (isWebm(inputPath)) {
    args.push("-fflags", "+genpts");
  }
  args.push(
    "-i",
    inputPath,
    "-an",
    "-vf",
    "scale=800:-1,fps=30",
    "-vframes",
    "1",
    "-q:v",
    "2",
    outputJpg,
  );
  await execFileAsync(FFMPEG_PATH, args, { maxBuffer: 10 * 1024 * 1024 });
}

function unlinkSceneFrames(outputDir, analysisId) {
  for (const f of listSceneFrames(outputDir, analysisId)) {
    try {
      fs.unlinkSync(f);
    } catch {
      /* ignore */
    }
  }
}

function unlinkMotionFrames(outputDir, analysisId) {
  for (const f of listMotionFrames(outputDir, analysisId)) {
    try {
      fs.unlinkSync(f);
    } catch {
      /* ignore */
    }
  }
}

/**
 * Evenly spaced frames across [0, duration).
 */
/**
 * Split [0, duration) into `count` equal segments; grab frame at the middle of each segment.
 * @param {string} filePrefix - e.g. "even_" or "uniform_"
 */
async function extractEvenlySpacedByDuration(
  inputPath,
  outputDir,
  analysisId,
  count,
  filePrefix = "even_",
) {
  const duration = await getDurationSeconds(inputPath);
  if (duration == null || duration <= 0) {
    return [];
  }
  const paths = [];
  for (let i = 0; i < count; i++) {
    const t = ((i + 0.5) * duration) / count;
    const out = path.join(
      outputDir,
      `${filePrefix}${analysisId}_${String(i + 1).padStart(3, "0")}.jpg`,
    );
    try {
      await extractSingleFrameAtTime(inputPath, out, t);
      paths.push(out);
    } catch {
      /* skip broken slice */
    }
  }
  return paths.filter((p) => fs.existsSync(p));
}

function unlinkPrefixedFrames(outputDir, analysisId, prefix) {
  let files = [];
  try {
    files = fs.readdirSync(outputDir);
  } catch {
    return;
  }
  const start = `${prefix}${analysisId}_`;
  for (const name of files) {
    if (!name.startsWith(start) || !name.endsWith(".jpg")) continue;
    try {
      fs.unlinkSync(path.join(outputDir, name));
    } catch {
      /* ignore */
    }
  }
}

async function extractAtFixedTimestamps(
  inputPath,
  outputDir,
  analysisId,
  timestamps,
) {
  const paths = [];
  for (let i = 0; i < timestamps.length; i++) {
    const t = timestamps[i];
    const out = path.join(
      outputDir,
      `fixed_${analysisId}_${String(i + 1).padStart(3, "0")}.jpg`,
    );
    try {
      await extractSingleFrameAtTime(inputPath, out, t);
      if (fs.existsSync(out)) paths.push(out);
    } catch {
      /* try next */
    }
  }
  return paths;
}

function normalizeMovementType(raw) {
  if (typeof raw !== "string" || !raw.trim()) return "default";
  const s = raw.trim().toLowerCase();
  if (s === "shooting") return "shooting";
  if (s === "deadlift") return "deadlift";
  if (s === "bench") return "bench";
  if (s === "ohp") return "ohp";
  if (s === "lunge") return "lunge";
  if (s === "rdl") return "rdl";
  return "default";
}

function fixedTimestampsForMovement(movementKey) {
  if (movementKey === "shooting") return FIXED_FALLBACK.shooting;
  if (movementKey === "deadlift") return FIXED_FALLBACK.deadlift;
  if (movementKey === "bench") return FIXED_FALLBACK.bench;
  if (movementKey === "ohp") return FIXED_FALLBACK.ohp;
  if (movementKey === "lunge") return FIXED_FALLBACK.lunge;
  if (movementKey === "rdl") return FIXED_FALLBACK.rdl;
  return FIXED_FALLBACK.default;
}

async function readFramesAsPayload(jpegPaths) {
  const frames = [];
  for (const p of jpegPaths) {
    const buf = await fs.promises.readFile(p);
    frames.push({
      base64: buf.toString("base64"),
      mediaType: "image/jpeg",
    });
  }
  return frames;
}

async function extractUniversalFrames(
  inputPath,
  analysisId,
  movementTypeRaw,
  movementPositionRaw,
) {
  let workingPath = inputPath;
  let normalizedPathToCleanup = null;
  workingPath = await normalizeVideo(inputPath, analysisId);
  if (workingPath !== inputPath) {
    normalizedPathToCleanup = workingPath;
  }

  const movementKey = normalizeMovementType(movementTypeRaw);
  const tmpRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), `frames_${analysisId}_`),
  );

  // Movement position window mapping
  const windowMap = {
    early: { start: 0.0, end: 0.6 },
    middle: { start: 0.2, end: 0.8 },
    late: { start: 0.4, end: 1.0 },
    unknown: { start: 0.0, end: 1.0 },
  };
  const mpKey =
    typeof movementPositionRaw === "string"
      ? movementPositionRaw.trim().toLowerCase()
      : "unknown";
  const movementPosition = windowMap[mpKey] ? mpKey : "unknown";
  const window = windowMap[movementPosition] ?? windowMap.unknown;

  console.log("[frames] movementPosition:", movementPosition);

  try {
    const lowerInput = inputPath.toLowerCase();
    console.log("[frames] input video info:", {
      filename: path.basename(inputPath),
      isScreenRecording:
        lowerInput.includes("screenrecording") ||
        lowerInput.includes("screen_recording"),
      sizeBytes: fs.statSync(inputPath).size,
    });

    const durationSecondsProbe = await getDurationSeconds(workingPath);
    const duration =
      durationSecondsProbe != null && durationSecondsProbe > 0
        ? durationSecondsProbe
        : 0;

    console.log("[frames] duration (normalized, sec):", durationSecondsProbe ?? 0);

    const windowStart = duration > 0 ? duration * window.start : 0;
    const windowDuration =
      duration > 0 ? duration * (window.end - window.start) : 999;

    const useWindowSeek = duration > 0 && movementPosition !== "unknown";

    console.log("[frames] window:", windowStart, "to", windowStart + windowDuration);

    const isScreenRecording = isScreenRecordingPath(inputPath);
    const scenePrimaryTh = isScreenRecording ? 0.05 : 0.15;
    const sceneSecondaryTh = isScreenRecording ? 0.04 : 0.08;

    let strategyName = "unknown";
    let jpegPaths = [];

    /**
     * Scene detection: windowed first (when applicable), then full clip if < 4 frames.
     */
    async function scenePassWindowThenFull(threshold) {
      unlinkSceneFrames(tmpRoot, analysisId);
      if (useWindowSeek) {
        try {
          await runSceneDetection(workingPath, tmpRoot, analysisId, threshold, {
            ss: windowStart,
            t: windowDuration,
          });
        } catch {
          /* may produce zero files */
        }
        let paths = listSceneFrames(tmpRoot, analysisId);
        if (paths.length < 4) {
          unlinkSceneFrames(tmpRoot, analysisId);
          try {
            await runSceneDetection(workingPath, tmpRoot, analysisId, threshold, null);
          } catch {
            /* */
          }
          paths = listSceneFrames(tmpRoot, analysisId);
        }
        return paths;
      }
      try {
        await runSceneDetection(workingPath, tmpRoot, analysisId, threshold, null);
      } catch {
        /* may produce zero files */
      }
      return listSceneFrames(tmpRoot, analysisId);
    }

    jpegPaths = await scenePassWindowThenFull(scenePrimaryTh);
    if (jpegPaths.length > 10) {
      jpegPaths = subsampleEvenly(jpegPaths, 10);
    }

    let sceneOk =
      jpegPaths.length >= 4 && !needsSceneFallback(jpegPaths);
    let usedSecondaryScene = false;

    if (!sceneOk) {
      unlinkSceneFrames(tmpRoot, analysisId);
      jpegPaths = await scenePassWindowThenFull(sceneSecondaryTh);
      if (jpegPaths.length > 10) {
        jpegPaths = subsampleEvenly(jpegPaths, 10);
      }
      sceneOk =
        jpegPaths.length >= 4 && !needsSceneFallback(jpegPaths);
      usedSecondaryScene = true;
    }

    if (sceneOk) {
      strategyName = usedSecondaryScene
        ? `scene_detection_${sceneSecondaryTh}`
        : `scene_detection_${scenePrimaryTh}`;
    } else {
      // --- Fallback: motion-style pass (scene + setpts), then uniform 10 segments, then fixed timestamps
      console.log(
        "[frames] scene insufficient or low-variance frames → motion / uniform fallback",
      );
      unlinkSceneFrames(tmpRoot, analysisId);
      unlinkMotionFrames(tmpRoot, analysisId);

      async function motionPassWindowThenFull() {
        unlinkMotionFrames(tmpRoot, analysisId);
        if (useWindowSeek) {
          try {
            await runMotionDetection(workingPath, tmpRoot, analysisId, {
              ss: windowStart,
              t: windowDuration,
            });
          } catch {
            /* */
          }
          let paths = listMotionFrames(tmpRoot, analysisId);
          if (paths.length < 4) {
            unlinkMotionFrames(tmpRoot, analysisId);
            try {
              await runMotionDetection(workingPath, tmpRoot, analysisId, null);
            } catch {
              /* */
            }
            paths = listMotionFrames(tmpRoot, analysisId);
          }
          return paths;
        }
        try {
          await runMotionDetection(workingPath, tmpRoot, analysisId, null);
        } catch {
          /* */
        }
        return listMotionFrames(tmpRoot, analysisId);
      }

      let motionPaths = await motionPassWindowThenFull();
      if (motionPaths.length > 10) {
        motionPaths = subsampleEvenly(motionPaths, 10);
      }

      if (motionPaths.length >= 4) {
        jpegPaths = motionPaths;
        strategyName = "motion_scene_0.08_setpts";
      } else {
        unlinkMotionFrames(tmpRoot, analysisId);
        unlinkPrefixedFrames(tmpRoot, analysisId, "uniform_");
        jpegPaths = await extractEvenlySpacedByDuration(
          workingPath,
          tmpRoot,
          analysisId,
          UNIFORM_SEGMENT_COUNT,
          "uniform_",
        );
        if (jpegPaths.length >= 4) {
          strategyName = "uniform_10_segments";
        } else {
          unlinkPrefixedFrames(tmpRoot, analysisId, "uniform_");
          jpegPaths = await extractAtFixedTimestamps(
            workingPath,
            tmpRoot,
            analysisId,
            fixedTimestampsForMovement(movementKey),
          );
          strategyName = `fixed_timestamps_${movementKey}`;
        }
      }
    }

    const frameCount = jpegPaths.length;
    console.log("[frames] strategy used:", strategyName);
    console.log("[frames] frames extracted:", frameCount);
    console.log("[frames] movement type:", movementTypeRaw ?? movementKey);

    if (frameCount === 0) {
      throw new Error("No frames could be extracted from video");
    }

    return await readFramesAsPayload(jpegPaths);
  } finally {
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch {
      /* */
    }
    if (normalizedPathToCleanup) {
      try {
        fs.rmSync(normalizedPathToCleanup, { force: true });
      } catch {
        /* */
      }
    }
  }
}

function getSupabase() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return createClient(url, key);
}

async function downloadVideoToTemp(storagePath, analysisId) {
  const supabase = getSupabase();
  const { data, error } = await supabase.storage
    .from("videos")
    .download(storagePath);
  if (error || !data) {
    throw new Error(error?.message || "Failed to download video from storage");
  }
  const buf = Buffer.from(await data.arrayBuffer());
  const ext = path.extname(storagePath) || ".mp4";
  const inputPath = path.join(
    os.tmpdir(),
    `input_${analysisId}${ext}`,
  );
  await fs.promises.writeFile(inputPath, buf);
  return inputPath;
}

const app = express();
app.use(express.json({ limit: "2mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/extract-frames", wrapHeavyHandler(async (req, res) => {
  const auth = req.headers.authorization || "";
  const secret = process.env.SERVICE_SECRET;
  if (!secret || auth !== `Bearer ${secret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { analysisId, storagePath, movementType, movementPosition } =
    req.body || {};
  if (typeof analysisId !== "string" || !analysisId.trim()) {
    return res.status(400).json({ error: "analysisId required" });
  }
  if (typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "storagePath required" });
  }

  let inputPath = null;
  try {
    inputPath = await downloadVideoToTemp(storagePath.trim(), analysisId.trim());
    const frames = await extractUniversalFrames(
      inputPath,
      analysisId.trim(),
      movementType,
      movementPosition ?? "unknown",
    );
    return res.json({ frames });
  } catch (err) {
    console.error("[extract-frames]", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Frame extraction failed",
    });
  } finally {
    if (inputPath && fs.existsSync(inputPath)) {
      try {
        fs.unlinkSync(inputPath);
      } catch {
        /* */
      }
    }
  }
}));
// ─── Game Film Frame Extraction ────────────────────────────────────────────

async function downloadGameVideoToTemp(storagePath, gameId) {
  const supabase = getSupabase();
  const { data, error } = await supabase.storage
    .from("game-videos")
    .download(storagePath);
  if (error || !data) {
    throw new Error(error?.message || "Failed to download game video from storage");
  }
  const buf = Buffer.from(await data.arrayBuffer());
  const ext = path.extname(storagePath) || ".mp4";
  const inputPath = path.join(os.tmpdir(), `game_input_${gameId}${ext}`);
  await fs.promises.writeFile(inputPath, buf);
  return inputPath;
}

async function extractGameFilmFrames(inputPath, gameId) {
  const workingPath = await normalizeVideo(inputPath, gameId);
  const normalizedToCleanup = workingPath !== inputPath ? workingPath : null;
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), `gameframes_${gameId}_`));

  try {
    const duration = await getDurationSeconds(workingPath);
    if (!duration || duration <= 0) {
      throw new Error("Could not determine game video duration");
    }

    console.log(`[game-frames] duration: ${duration}s (${Math.round(duration / 60)}min)`);

    // Strategy: 1 frame per 10 seconds for full game
    // Plus denser coverage (1 per 3s) for first 5 minutes to capture lineups
    const frames = [];
    const addedTimes = new Set();

    // Dense first-5-min coverage (lineup identification)
    const denseEnd = Math.min(300, duration);
    for (let t = 3; t < denseEnd; t += 3) {
      const rounded = Math.round(t);
      if (!addedTimes.has(rounded)) {
        frames.push(rounded);
        addedTimes.add(rounded);
      }
    }

    // 1 frame per 10 seconds for remainder
    for (let t = 300; t < duration; t += 10) {
      const rounded = Math.round(t);
      if (!addedTimes.has(rounded)) {
        frames.push(rounded);
        addedTimes.add(rounded);
      }
    }

    frames.sort((a, b) => a - b);
    console.log(`[game-frames] extracting ${frames.length} frames`);

    // Extract frames in batches of 20 to avoid memory pressure
    const jpegPaths = [];
    for (let i = 0; i < frames.length; i++) {
      const t = frames[i];
      const out = path.join(
        tmpRoot,
        `game_${gameId}_${String(i + 1).padStart(4, "0")}_t${t}.jpg`
      );
      try {
        await extractSingleFrameAtTime(workingPath, out, t);
        if (fs.existsSync(out)) {
          jpegPaths.push({ path: out, timestamp_seconds: t });
        }
      } catch {
        /* skip failed frame */
      }
    }

    console.log(`[game-frames] extracted ${jpegPaths.length} frames successfully`);

    if (jpegPaths.length === 0) {
      throw new Error("No frames extracted from game video");
    }

    // Read as base64 payload — include timestamp metadata
    const payload = [];
    for (const { path: p, timestamp_seconds } of jpegPaths) {
      const buf = await fs.promises.readFile(p);
      payload.push({
        base64: buf.toString("base64"),
        mediaType: "image/jpeg",
        timestamp_seconds,
      });
    }

    return payload;

  } finally {
    try { fs.rmSync(tmpRoot, { recursive: true, force: true }); } catch { /* */ }
    if (normalizedToCleanup) {
      try { fs.rmSync(normalizedToCleanup, { force: true }); } catch { /* */ }
    }
  }
}

app.post("/extract-game-film-frames", wrapHeavyHandler(async (req, res) => {
  const auth = req.headers.authorization || "";
  const secret = process.env.FRAMES_SERVICE_SECRET;
  if (!secret || auth !== `Bearer ${secret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { gameId, storagePath } = req.body || {};
  if (typeof gameId !== "string" || !gameId.trim()) {
    return res.status(400).json({ error: "gameId required" });
  }
  if (typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "storagePath required" });
  }

  let inputPath = null;
  try {
    console.log(`[game-frames] starting extraction for game ${gameId}`);
    inputPath = await downloadGameVideoToTemp(storagePath.trim(), gameId.trim());
    const frames = await extractGameFilmFrames(inputPath, gameId.trim());
    console.log(`[game-frames] complete: ${frames.length} frames for game ${gameId}`);
    return res.json({ frames, frame_count: frames.length });
  } catch (err) {
    console.error("[extract-game-film-frames]", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Game film extraction failed",
    });
  } finally {
    if (inputPath && fs.existsSync(inputPath)) {
      try { fs.unlinkSync(inputPath); } catch { /* */ }
    }
  }
}));

app.post("/extract-game-film-crops", wrapHeavyHandler(async (req, res) => {
  const auth = req.headers.authorization || "";
  const secret = process.env.FRAMES_SERVICE_SECRET;
  if (!secret || auth !== `Bearer ${secret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const startedAt = Date.now();
  const { gameId, storagePath, crops } = req.body || {};
  if (typeof gameId !== "string" || !gameId.trim()) {
    return res.status(400).json({ error: "gameId required" });
  }
  if (typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "storagePath required" });
  }
  if (!Array.isArray(crops)) {
    return res.status(400).json({ error: "crops array required" });
  }
  if (crops.length > GAME_CROPS_MAX_REQUEST) {
    return res.status(400).json({
      error: `crops cannot exceed ${GAME_CROPS_MAX_REQUEST}`,
    });
  }

  const requestId = randomUUID();
  console.log("[extract-game-film-crops] request start", {
    gameId: gameId.trim(),
    storagePath: storagePath.trim(),
    cropsRequested: crops.length,
    requestId,
  });

  let inputPath = null;
  try {
    const downloadStart = Date.now();
    inputPath = await streamDownloadGameVideoToTemp(
      storagePath.trim(),
      gameId.trim(),
      requestId,
    );
    const downloadMs = Date.now() - downloadStart;

    const probeStart = Date.now();
    const meta = await getVideoMetadata(inputPath);
    const probeMs = Date.now() - probeStart;
    console.log("[extract-game-film-crops] video metadata", {
      requestId,
      width: meta.width,
      height: meta.height,
      fps: Number(meta.fps.toFixed(3)),
      duration_sec: Number(meta.durationSec.toFixed(3)),
      download_ms: downloadMs,
      probe_ms: probeMs,
    });

    let skipped = 0;
    const out = [];
    for (let idx = 0; idx < crops.length; idx++) {
      const item = crops[idx];
      if (!item || typeof item !== "object") {
        skipped += 1;
        console.warn("[extract-game-film-crops] skipped non-object crop", {
          requestId,
          index: idx,
        });
        continue;
      }
      const trackIdRaw = item.track_id;
      const trackId = Number.isFinite(Number(trackIdRaw))
        ? Math.trunc(Number(trackIdRaw))
        : null;
      const timestamp = Number(item.timestamp);
      if (!(trackId !== null && Number.isFinite(timestamp) && timestamp >= 0)) {
        skipped += 1;
        console.warn("[extract-game-film-crops] skipped invalid track/timestamp", {
          requestId,
          index: idx,
          track_id: trackIdRaw,
          timestamp: item.timestamp,
        });
        continue;
      }

      const warnings = [];
      const bbox = item.bbox;
      const centerX = item.center_x;
      const centerY = item.center_y;

      let window = null;
      if (bbox !== null && hasValidBbox(bbox)) {
        window = buildBboxExactWindow(bbox, meta);
      } else if (centerX !== null && centerY !== null) {
        window = buildCenterHeuristicWindow(Number(centerX), Number(centerY), meta);
      }

      if (!window) {
        skipped += 1;
        console.warn("[extract-game-film-crops] skipped unresolved crop window", {
          requestId,
          index: idx,
          track_id: trackId,
        });
        continue;
      }

      let imageBase64 = "";
      let cropWidth = Math.max(0, window.x2 - window.x1);
      let cropHeight = Math.max(0, window.y2 - window.y1);
      let ffmpegMs = 0;

      try {
        const cropResult = await runFfmpegCropToBuffer({
          inputPath,
          timestamp,
          window,
        });
        ffmpegMs = cropResult.elapsedMs;
        imageBase64 = cropResult.buffer.toString("base64");
        const jpegSize = parseJpegDimensions(cropResult.buffer);
        if (jpegSize) {
          cropWidth = jpegSize.width;
          cropHeight = jpegSize.height;
        }
        if (cropWidth < GAME_CROPS_MIN_EDGE_PX || cropHeight < GAME_CROPS_MIN_EDGE_PX) {
          warnings.push(
            `small_output_${cropWidth}x${cropHeight}_below_${GAME_CROPS_MIN_EDGE_PX}px`,
          );
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        warnings.push(`ffmpeg_error: ${msg}`);
        console.error("[extract-game-film-crops] ffmpeg crop failed", {
          requestId,
          index: idx,
          track_id: trackId,
          method: window.method,
          error: msg,
        });
      }

      console.log("[extract-game-film-crops] crop", {
        requestId,
        index: idx,
        track_id: trackId,
        timestamp,
        method: window.method,
        crop_width: cropWidth,
        crop_height: cropHeight,
        ffmpeg_ms: ffmpegMs,
      });

      out.push({
        track_id: trackId,
        timestamp,
        method: window.method,
        image_base64: imageBase64,
        crop_width: cropWidth,
        crop_height: cropHeight,
        warnings,
      });
    }

    const elapsedMs = Date.now() - startedAt;
    console.log("[extract-game-film-crops] request complete", {
      requestId,
      crops_processed: out.length,
      crops_skipped: skipped,
      elapsed_ms: elapsedMs,
    });

    return res.json({
      crops_processed: out.length,
      crops_skipped: skipped,
      video_metadata: {
        width: meta.width,
        height: meta.height,
        fps: Number(meta.fps.toFixed(3)),
        duration_sec: Number(meta.durationSec.toFixed(3)),
      },
      crops: out,
    });
  } catch (err) {
    console.error("[extract-game-film-crops]", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Game crop extraction failed",
    });
  } finally {
    if (inputPath && fs.existsSync(inputPath)) {
      try {
        fs.unlinkSync(inputPath);
      } catch {
        /* */
      }
    }
  }
}));

async function detectMotionIntensity(inputPath, startSec, endSec) {
  // Use ffmpeg signalstats filter to measure pixel-level activity.
  // Higher activity = more motion = more action.
  // Returns 0.0-1.0 normalized intensity.
  return new Promise((resolve, reject) => {
    const duration = endSec - startSec;
    if (duration < 0.5) {
      resolve(0.3);
      return;
    }

    let stderr = "";
    const proc = spawn(FFMPEG_PATH, [
      "-hide_banner",
      "-loglevel", "info",
      "-ss", String(startSec),
      "-t", String(duration),
      "-i", inputPath,
      "-an",
      "-vf", "scale=320:180,signalstats,metadata=print",
      "-f", "null",
      "-",
    ]);

    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 500_000) {
        stderr = "...[truncated]..." + stderr.slice(-200_000);
      }
    });

    proc.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`motion detect ffmpeg exited ${code}`));
        return;
      }

      // Parse YDIF (Y-channel frame difference) values from stderr.
      // High YDIF = lots of frame-to-frame change = high motion.
      const ydifMatches = stderr.matchAll(/lavfi\.signalstats\.YDIF=([0-9.]+)/g);
      const ydifValues = [];
      for (const m of ydifMatches) {
        const v = parseFloat(m[1]);
        if (!isNaN(v)) ydifValues.push(v);
      }

      if (ydifValues.length === 0) {
        resolve(0.3); // No data, return default
        return;
      }

      // Average YDIF, normalize to 0-1
      // Empirical calibration from Mason 21s wide-angle basketball clip:
      // YDIF range 2.7-4.2 mapped through divisor=5 gives 0.54-0.84
      // (transition to action range — appropriate for active play)
      // Note: wide-angle shots dilute YDIF (static background pixels
      // average down with player motion). Future: clip-type-aware
      // calibration via S-clip-type-classification (parking lot).
      // Tunable as we gather data from diverse upload types.
      const avgYdif = ydifValues.reduce((a, b) => a + b, 0) / ydifValues.length;
      // Sort for percentile snapshot.
      const sorted = [...ydifValues].sort((a, b) => a - b);
      const p10 = sorted[Math.floor(sorted.length * 0.1)] ?? 0;
      const p50 = sorted[Math.floor(sorted.length * 0.5)] ?? 0;
      const p90 = sorted[Math.floor(sorted.length * 0.9)] ?? 0;
      const ymax = sorted[sorted.length - 1] ?? 0;
      const normalized = Math.min(1.0, avgYdif / 5.0);
      // S-frame-service-ydif-instrumentation: log raw distribution for
      // calibration. Today's divisor=5 was tuned on a 21s Mason clip.
      // Need cross-clip data before retuning.
      console.log("[motion-intensity]", JSON.stringify({
        startSec: Math.round(startSec * 10) / 10,
        endSec: Math.round(endSec * 10) / 10,
        sample_count: ydifValues.length,
        avg: Math.round(avgYdif * 100) / 100,
        p10: Math.round(p10 * 100) / 100,
        p50: Math.round(p50 * 100) / 100,
        p90: Math.round(p90 * 100) / 100,
        max: Math.round(ymax * 100) / 100,
        normalized: Math.round(normalized * 100) / 100,
      }));
      resolve(normalized);
    });

    proc.on("error", reject);
  });
}

async function detectSceneWindows(inputPath, gameId) {
  const workingPath = await normalizeVideo(inputPath, gameId);
  const normalizedToCleanup = workingPath !== inputPath ? workingPath : null;
  try {
    const duration = await getDurationSeconds(workingPath);
    if (!duration || duration <= 0) {
      throw new Error("Could not determine video duration");
    }
    if (duration > 7200) {
      throw new Error(
        `Video too long: ${Math.round(duration)}s (max 7200s / 2hr). Contact support for longer videos.`,
      );
    }
    console.log(`[detect-scenes] scene_threshold=0.08 duration=${Math.round(duration)}s for ${gameId}`);

    const sceneChanges = [];
    let fullStderr = "";
    let stderrBuf = "";

    await new Promise((resolve, reject) => {
      const proc = spawn(FFMPEG_PATH, [
        "-hide_banner",
        "-loglevel", "info",
        "-i", workingPath,
        "-an",
        "-filter:v", "select='gt(scene,0.08)',showinfo",
        "-f", "null",
        "-"
      ]);
      proc.stderr.on("data", (chunk) => {
        const text = chunk.toString();
        fullStderr += text;
        if (fullStderr.length > 1_000_000) {
          fullStderr = "...[truncated]..." + fullStderr.slice(-500_000);
        }
        stderrBuf += text;
        const matches = stderrBuf.matchAll(/pts_time:([0-9.]+)/g);
        for (const m of matches) {
          const t = parseFloat(m[1]);
          if (!isNaN(t) && !sceneChanges.includes(t)) sceneChanges.push(t);
        }
        // Drain at last newline to avoid re-matching
        const lastNewline = stderrBuf.lastIndexOf("\n");
        if (lastNewline > 0) stderrBuf = stderrBuf.slice(lastNewline);
      });
      proc.on("close", (code) => {
        if (code === 0) {
          resolve();
        } else {
          const tail = fullStderr.slice(-500);
          reject(new Error(`ffmpeg scene detect exited ${code}. stderr tail: ${tail}`));
        }
      });
      proc.on("error", reject);
    });

    sceneChanges.sort((a, b) => a - b);
    console.log(`[detect-scenes] ${sceneChanges.length} scene change events for ${gameId}`);

    // Group scene changes into play windows
    const PLAY_GAP_SEC = 3;
    const MIN_WINDOW_SEC = 3;
    const MAX_WINDOW_SEC = 15;
    const LEAD_IN_SEC = 1;
    const LEAD_OUT_SEC = 2;

    const rawWindows = [];
    let windowStart = null;
    let lastChange = null;
    let eventCount = 0;

    for (const t of sceneChanges) {
      if (windowStart === null) {
        windowStart = t;
        lastChange = t;
        eventCount = 1;
        continue;
      }
      if (t - lastChange > PLAY_GAP_SEC) {
        rawWindows.push({
          firstChange: windowStart,
          lastChange,
          eventCount,
        });
        windowStart = t;
        eventCount = 1;
      } else {
        eventCount += 1;
      }
      lastChange = t;
    }
    if (windowStart !== null && lastChange !== null) {
      rawWindows.push({
        firstChange: windowStart,
        lastChange,
        eventCount,
      });
    }

    // Apply lead-in/out, clamp to duration, split long windows, filter short ones
    const scenes = [];
    for (const w of rawWindows) {
      const start = Math.max(0, w.firstChange - LEAD_IN_SEC);
      const end = Math.min(duration, w.lastChange + LEAD_OUT_SEC);
      const span = end - start;
      if (span < MIN_WINDOW_SEC) continue;

      // Intensity: events per second, normalized to 0.0-1.0
      // 2+ events/sec = max intensity (1.0); tunable.
      const eventsPerSecond = w.eventCount / Math.max(span, 0.1);
      const intensityScore = Math.min(1.0, eventsPerSecond / 2.0);

      // Scene type heuristic from intensity
      const sceneType =
        intensityScore >= 0.7
          ? "action"
          : intensityScore >= 0.4
            ? "transition"
            : "uniform";

      if (span <= MAX_WINDOW_SEC) {
        scenes.push({
          start_sec: Math.round(start * 10) / 10,
          end_sec: Math.round(end * 10) / 10,
          confidence: 1.0,
          intensity_score: Math.round(intensityScore * 100) / 100,
          scene_change_count: w.eventCount,
          duration_sec: Math.round(span * 10) / 10,
          scene_type: sceneType,
        });
      } else {
        // Split long windows; intensity divided across halves
        const mid = start + span / 2;
        const halfSpan = span / 2;
        const halfEvents = Math.round(w.eventCount / 2);
        const halfEventsPerSec = halfEvents / Math.max(halfSpan, 0.1);
        const halfIntensity = Math.min(1.0, halfEventsPerSec / 2.0);
        const halfSceneType =
          halfIntensity >= 0.7
            ? "action"
            : halfIntensity >= 0.4
              ? "transition"
              : "uniform";

        scenes.push({
          start_sec: Math.round(start * 10) / 10,
          end_sec: Math.round(mid * 10) / 10,
          confidence: 0.8,
          intensity_score: Math.round(halfIntensity * 100) / 100,
          scene_change_count: halfEvents,
          duration_sec: Math.round(halfSpan * 10) / 10,
          scene_type: halfSceneType,
        });
        scenes.push({
          start_sec: Math.round(mid * 10) / 10,
          end_sec: Math.round(end * 10) / 10,
          confidence: 0.8,
          intensity_score: Math.round(halfIntensity * 100) / 100,
          scene_change_count: halfEvents,
          duration_sec: Math.round(halfSpan * 10) / 10,
          scene_type: halfSceneType,
        });
      }
    }

    console.log(`[detect-scenes] produced ${scenes.length} play windows for ${gameId}`);
    if (scenes.length === 0) {
      console.log(`[detect-scenes] no scene changes — falling back to uniform 10s windows for ${gameId}`);
      const WINDOW_SEC = 10;
      const OVERLAP_SEC = 2;
      const STEP = WINDOW_SEC - OVERLAP_SEC;

      const enableMotionDetection =
        process.env.ENABLE_MOTION_DETECTION_FALLBACK !== "false";

      const motionStartTime = Date.now();
      const candidateWindows = [];
      for (let t = 0; t < duration; t += STEP) {
        const start = t;
        const end = Math.min(duration, t + WINDOW_SEC);
        if (end - start < 3) continue;
        candidateWindows.push({ start, end });
      }

      console.log(
        `[detect-scenes] fallback: ${candidateWindows.length} candidate windows, motion_detection=${enableMotionDetection}`,
      );

      for (const w of candidateWindows) {
        let intensityScore = 0.3; // default fallback intensity
        let sceneType = "uniform";

        if (enableMotionDetection) {
          try {
            // Run lightweight motion detection on this window
            // Use ffmpeg signalstats to measure pixel-level activity
            const motionScore = await detectMotionIntensity(
              workingPath,
              w.start,
              w.end,
            );
            intensityScore = motionScore;
            sceneType =
              motionScore >= 0.7
                ? "action"
                : motionScore >= 0.4
                  ? "transition"
                  : "uniform";
          } catch (motionErr) {
            console.log(
              `[detect-scenes] motion detection failed for window ${w.start}-${w.end}: ${motionErr.message}; using default 0.3`,
            );
          }
        }

        scenes.push({
          start_sec: Math.round(w.start * 10) / 10,
          end_sec: Math.round(w.end * 10) / 10,
          confidence: 0.5,
          source: "uniform_fallback",
          intensity_score: Math.round(intensityScore * 100) / 100,
          duration_sec: Math.round((w.end - w.start) * 10) / 10,
          scene_type: sceneType,
        });
      }

      const motionElapsedSec = (Date.now() - motionStartTime) / 1000;
      console.log(
        `[detect-scenes] fallback produced ${scenes.length} windows in ${motionElapsedSec.toFixed(1)}s ` +
          `(motion_detection=${enableMotionDetection})`,
      );
    }
    return scenes;
  } finally {
    if (normalizedToCleanup) {
      try { fs.rmSync(normalizedToCleanup, { force: true }); } catch { /* */ }
    }
  }
}

app.post("/detect-scenes", wrapHeavyHandler(async (req, res) => {
  const auth = req.headers.authorization || "";
  const secret = process.env.FRAMES_SERVICE_SECRET;
  if (!secret || auth !== `Bearer ${secret}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  const { gameId, storagePath } = req.body || {};
  if (typeof gameId !== "string" || !gameId.trim()) {
    return res.status(400).json({ error: "gameId required" });
  }
  if (typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "storagePath required" });
  }

  let inputPath = null;
  try {
    console.log(`[detect-scenes] starting for game ${gameId}`);
    inputPath = await downloadGameVideoToTemp(storagePath.trim(), gameId.trim());
    const scenes = await detectSceneWindows(inputPath, gameId.trim());
    console.log(`[detect-scenes] complete: ${scenes.length} windows for ${gameId}`);

    // Compute response metadata
    const totalSceneChanges = scenes.reduce(
      (sum, s) => sum + (s.scene_change_count || 0),
      0,
    );
    const fallbackUsed = scenes.some((s) => s.source === "uniform_fallback");
    const sceneDetected = scenes.some((s) => s.source !== "uniform_fallback");
    const detectionMethod =
      fallbackUsed && sceneDetected
        ? "mixed"
        : fallbackUsed
          ? "uniform_fallback"
          : "scene_detection";

    // Get duration from last scene end (timeline coverage for sampling)
    const lastScene = scenes[scenes.length - 1];
    const durationSec = lastScene ? lastScene.end_sec : 0;

    return res.json({
      scenes,
      scene_count: scenes.length,
      duration_sec: durationSec,
      total_scene_changes: totalSceneChanges,
      detection_method: detectionMethod,
    });
  } catch (err) {
    console.error("[detect-scenes] error", err);
    return res.status(500).json({
      error: err instanceof Error ? err.message : "Scene detection failed",
    });
  } finally {
    if (inputPath && fs.existsSync(inputPath)) {
      try { fs.unlinkSync(inputPath); } catch { /* */ }
    }
  }
}));

app.listen(PORT, () => {
  console.log(`[frames] listening on ${PORT}`);
});
