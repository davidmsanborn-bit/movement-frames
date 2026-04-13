/**
 * MOTIQ — frame extraction service (Railway / Node)
 * Universal motion-based strategy for all movement types.
 */

const express = require("express");
const fs = require("fs");
const os = require("os");
const path = require("path");
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

app.post("/extract-frames", async (req, res) => {
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
});
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

app.post("/extract-game-film-frames", async (req, res) => {
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
});

async function detectSceneWindows(inputPath, gameId) {
  const workingPath = await normalizeVideo(inputPath, gameId);
  const normalizedToCleanup = workingPath !== inputPath ? workingPath : null;
  try {
    const duration = await getDurationSeconds(workingPath);
    if (!duration || duration <= 0) {
      throw new Error("Could not determine video duration");
    }
    if (duration > 1800) {
      throw new Error(
        `Video too long for Sprint 28 pipeline: ${Math.round(duration)}s (max 1800s / 30min). Shorter clips supported; longer game films in Sprint 29+.`,
      );
    }

    const sceneChanges = [];
    let fullStderr = "";
    let stderrBuf = "";

    await new Promise((resolve, reject) => {
      const proc = spawn(FFMPEG_PATH, [
        "-hide_banner",
        "-loglevel", "info",
        "-i", workingPath,
        "-filter:v", "select='gt(scene,0.2)',showinfo",
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

    for (const t of sceneChanges) {
      if (windowStart === null) {
        windowStart = t;
        lastChange = t;
        continue;
      }
      if (t - lastChange > PLAY_GAP_SEC) {
        rawWindows.push({ firstChange: windowStart, lastChange });
        windowStart = t;
      }
      lastChange = t;
    }
    if (windowStart !== null && lastChange !== null) {
      rawWindows.push({ firstChange: windowStart, lastChange });
    }

    // Apply lead-in/out, clamp to duration, split long windows, filter short ones
    const scenes = [];
    for (const w of rawWindows) {
      const start = Math.max(0, w.firstChange - LEAD_IN_SEC);
      const end = Math.min(duration, w.lastChange + LEAD_OUT_SEC);
      const span = end - start;
      if (span < MIN_WINDOW_SEC) continue;

      if (span <= MAX_WINDOW_SEC) {
        scenes.push({
          start_sec: Math.round(start * 10) / 10,
          end_sec: Math.round(end * 10) / 10,
          confidence: 1.0
        });
      } else {
        const mid = start + span / 2;
        scenes.push({
          start_sec: Math.round(start * 10) / 10,
          end_sec: Math.round(mid * 10) / 10,
          confidence: 0.8
        });
        scenes.push({
          start_sec: Math.round(mid * 10) / 10,
          end_sec: Math.round(end * 10) / 10,
          confidence: 0.8
        });
      }
    }

    console.log(`[detect-scenes] produced ${scenes.length} play windows for ${gameId}`);
    return scenes;
  } finally {
    if (normalizedToCleanup) {
      try { fs.rmSync(normalizedToCleanup, { force: true }); } catch { /* */ }
    }
  }
}

app.post("/detect-scenes", async (req, res) => {
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
    return res.json({ scenes, scene_count: scenes.length });
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
});

app.listen(PORT, () => {
  console.log(`[frames] listening on ${PORT}`);
});
