const express = require("express");
const { createClient } = require("@supabase/supabase-js");
const fs = require("fs/promises");
const fssync = require("fs");
const path = require("path");
const { execFile } = require("child_process");
const { promisify } = require("util");

const execFileAsync = promisify(execFile);

const PORT = Number(process.env.PORT) || 3001;
const SERVICE_SECRET = process.env.SERVICE_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "videos";

/** Default `/usr/bin/ffmpeg` on Linux/Railway; override with FFMPEG_PATH */
const FFMPEG = process.env.FFMPEG_PATH || "/usr/bin/ffmpeg";

const SCENE_MIN_FRAMES = 4;
const SCENE_MAX_FRAMES = 10;
const EVEN_SPREAD_COUNT = 8;

const FIXED_TIMESTAMPS = {
  shooting: [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
  deadlift: [1, 2, 3, 4, 5, 6, 7, 8],
  default: [1, 2, 3],
};

function isSafeId(value) {
  return typeof value === "string" && /^[a-zA-Z0-9._-]+$/.test(value) && value.length <= 256;
}

function authOk(req) {
  if (!SERVICE_SECRET) return false;
  const h = req.headers.authorization;
  if (typeof h !== "string") return false;
  return h === SERVICE_SECRET || h === `Bearer ${SERVICE_SECRET}`;
}

/** Browser-compressed webm often needs PTS regeneration for reliable seeks */
function isWebmPath(p) {
  return typeof p === "string" && p.toLowerCase().endsWith(".webm");
}

/** @param {boolean} isWebm */
function inputPrefixFlags(isWebm) {
  return isWebm ? ["-fflags", "+genpts"] : [];
}

/**
 * METHOD 1 — Duration from ffmpeg -i stderr
 * @param {string} combined
 * @returns {number | null}
 */
function parseDurationFromCombined(combined) {
  const durationMatch = combined.match(/Duration:\s*(\d+):(\d+):(\d+\.?\d*)/);
  if (!durationMatch) return null;
  const hours = parseFloat(durationMatch[1]);
  const minutes = parseFloat(durationMatch[2]);
  const seconds = parseFloat(durationMatch[3]);
  const durationSec = hours * 3600 + minutes * 60 + seconds;
  return Number.isFinite(durationSec) ? durationSec : null;
}

/**
 * METHOD 2 — decode to null; last frame= and fps= from stats
 * @param {string} inputPath
 * @param {boolean} isWebm
 * @returns {Promise<number | null>}
 */
async function getDurationFromFrameCount(inputPath, isWebm) {
  const pre = inputPrefixFlags(isWebm);
  const nullSink = process.platform === "win32" ? "NUL" : "/dev/null";
  const args = [...pre, "-i", inputPath, "-map", "0:v:0", "-c", "copy", "-f", "null", nullSink];
  let combined = "";
  try {
    const r = await execFileAsync(FFMPEG, args, { maxBuffer: 50 * 1024 * 1024 });
    const stderr = r.stderr != null ? Buffer.from(r.stderr).toString() : "";
    const stdout = r.stdout != null ? Buffer.from(r.stdout).toString() : "";
    combined = stderr + stdout;
  } catch (err) {
    const stderr = err.stderr != null ? Buffer.from(err.stderr).toString() : "";
    const stdout = err.stdout != null ? Buffer.from(err.stdout).toString() : "";
    combined = stderr + stdout;
  }

  let lastFrame = null;
  let lastFps = null;
  const lines = combined.split(/\r?\n/);
  for (const line of lines) {
    const fm = line.match(/frame=\s*(\d+)/);
    if (fm) lastFrame = parseInt(fm[1], 10);
    const fp = line.match(/fps=\s*([\d.]+)/);
    if (fp) lastFps = parseFloat(fp[1]);
  }

  if (lastFrame == null || !Number.isFinite(lastFrame) || lastFrame < 0) return null;
  if (lastFps == null || !Number.isFinite(lastFps) || lastFps <= 0) return null;
  const durationSec = lastFrame / lastFps;
  return Number.isFinite(durationSec) && durationSec > 0 ? durationSec : null;
}

/**
 * @param {string} inputPath
 * @param {boolean} isWebm
 * @returns {Promise<number | null>}
 */
async function getVideoDurationSec(inputPath, isWebm) {
  let stderr = "";
  let stdout = "";
  try {
    const r = await execFileAsync(FFMPEG, [...inputPrefixFlags(isWebm), "-i", inputPath], {
      maxBuffer: 10 * 1024 * 1024,
    });
    stderr = r.stderr != null ? Buffer.from(r.stderr).toString() : "";
    stdout = r.stdout != null ? Buffer.from(r.stdout).toString() : "";
  } catch (err) {
    stderr = err.stderr != null ? Buffer.from(err.stderr).toString() : "";
    stdout = err.stdout != null ? Buffer.from(err.stdout).toString() : "";
  }

  console.log("[frames] ffmpeg stderr:", stderr.slice(0, 500));

  const combined = stderr + stdout;
  const d1 = parseDurationFromCombined(combined);
  if (d1 != null && d1 > 0) {
    console.log("[frames] duration method:", "ffmpeg-i-duration", d1);
    return d1;
  }

  const d2 = await getDurationFromFrameCount(inputPath, isWebm);
  if (d2 != null && d2 > 0) {
    console.log("[frames] duration method:", "ffmpeg-null-framecount", d2);
    return d2;
  }

  console.log("[frames] duration method:", "failed", null);
  return null;
}

/** @param {string[]} paths @param {number} max */
function subsampleEvenly(paths, max) {
  if (paths.length <= max) return paths;
  const n = paths.length;
  const out = [];
  for (let k = 0; k < max; k++) {
    const idx = Math.round((k * (n - 1)) / (max - 1 || 1));
    out.push(paths[idx]);
  }
  return out;
}

/** @param {string} sceneFramesDir */
async function listSceneFrameJpegs(sceneFramesDir) {
  let entries = [];
  try {
    entries = await fs.readdir(sceneFramesDir);
  } catch {
    return [];
  }
  return entries
    .filter((f) => /^frame_\d+\.jpg$/i.test(f))
    .sort((a, b) => {
      const na = parseInt(String(a.match(/\d+/)?.[0] ?? "0"), 10);
      const nb = parseInt(String(b.match(/\d+/)?.[0] ?? "0"), 10);
      return na - nb;
    })
    .map((f) => path.join(sceneFramesDir, f));
}

async function unlinkMatching(sceneFramesDir, regex) {
  let entries = [];
  try {
    entries = await fs.readdir(sceneFramesDir);
  } catch {
    return;
  }
  for (const f of entries) {
    if (!regex.test(f)) continue;
    try {
      await fs.unlink(path.join(sceneFramesDir, f));
    } catch {
      /* ignore */
    }
  }
}

/**
 * STEP 1 / 2 — scene detection into sceneFramesDir/frame_%03d.jpg
 * @param {number} threshold e.g. 0.15 or 0.08
 */
async function runSceneDetection(inputPath, sceneFramesDir, threshold, isWebm) {
  const outPattern = path.join(sceneFramesDir, "frame_%03d.jpg");
  const vf = `select=gt(scene\\,${threshold}),scale=800:-1,fps=30`;
  const args = ["-y", "-hide_banner", "-loglevel", "error"];
  if (isWebm) args.push("-fflags", "+genpts");
  args.push(
    "-i",
    inputPath,
    "-vf",
    vf,
    "-vsync",
    "vfr",
    "-pix_fmt",
    "yuvj420p",
    "-q:v",
    "2",
    outPattern,
  );
  await execFileAsync(FFMPEG, args, { maxBuffer: 50 * 1024 * 1024 });
}

async function extractOneFrameAtTime(inputPath, outputJpg, timeSeconds, isWebm) {
  const args = ["-y", "-hide_banner", "-loglevel", "error", "-ss", String(timeSeconds)];
  if (isWebm) args.push("-fflags", "+genpts");
  args.push(
    "-i",
    inputPath,
    "-an",
    "-vf",
    "scale=800:-1,fps=30",
    "-vframes",
    "1",
    "-pix_fmt",
    "yuvj420p",
    "-q:v",
    "2",
    outputJpg,
  );
  await execFileAsync(FFMPEG, args, { maxBuffer: 50 * 1024 * 1024 });
}

/** STEP 3 — 8 segment midpoints */
async function extractEvenlySpaced(inputPath, sceneFramesDir, duration, isWebm) {
  const paths = [];
  for (let i = 0; i < EVEN_SPREAD_COUNT; i++) {
    const t = ((i + 0.5) * duration) / EVEN_SPREAD_COUNT;
    const out = path.join(sceneFramesDir, `even_${String(i + 1).padStart(3, "0")}.jpg`);
    try {
      await extractOneFrameAtTime(inputPath, out, t, isWebm);
      paths.push(out);
    } catch {
      /* skip */
    }
  }
  return paths.filter((p) => fssync.existsSync(p));
}

/** STEP 4 */
async function extractFixedTimestamps(inputPath, sceneFramesDir, times, isWebm) {
  const paths = [];
  for (let i = 0; i < times.length; i++) {
    const out = path.join(sceneFramesDir, `fixed_${String(i + 1).padStart(3, "0")}.jpg`);
    try {
      await extractOneFrameAtTime(inputPath, out, times[i], isWebm);
      if (fssync.existsSync(out)) paths.push(out);
    } catch {
      /* skip */
    }
  }
  return paths;
}

/** @param {unknown} movementType */
function fixedTimestampsForMovement(movementType) {
  const mt = movementType == null ? "" : String(movementType).toLowerCase().trim();
  if (mt === "shooting" || mt === "basketball") return [...FIXED_TIMESTAMPS.shooting];
  if (mt === "deadlift" || mt === "rdl") return [...FIXED_TIMESTAMPS.deadlift];
  return [...FIXED_TIMESTAMPS.default];
}

/**
 * Universal motion-based extraction for all movement types.
 * @returns {Promise<string[]>}
 */
async function extractFramePathsUniversal(inputPath, analysisId, movementType, isWebm) {
  const sceneFramesDir = `/tmp/scene_${analysisId}`;
  fssync.mkdirSync(sceneFramesDir, { recursive: true });

  let framePaths = [];

  // STEP 1 — scene 0.15
  try {
    await runSceneDetection(inputPath, sceneFramesDir, 0.15, isWebm);
  } catch (err) {
    console.error("[extract-frames] scene detection (0.15) failed:", err.message);
  }
  framePaths = await listSceneFrameJpegs(sceneFramesDir);
  if (framePaths.length > SCENE_MAX_FRAMES) {
    framePaths = subsampleEvenly(framePaths, SCENE_MAX_FRAMES);
  }
  if (framePaths.length >= SCENE_MIN_FRAMES) {
    console.log("[frames] strategy: scene-detection, count:", framePaths.length);
    return framePaths;
  }

  // STEP 2 — scene 0.08
  await unlinkMatching(sceneFramesDir, /^frame_\d+\.jpg$/i);
  try {
    await runSceneDetection(inputPath, sceneFramesDir, 0.08, isWebm);
  } catch (err) {
    console.error("[extract-frames] scene detection (0.08) failed:", err.message);
  }
  framePaths = await listSceneFrameJpegs(sceneFramesDir);
  if (framePaths.length > SCENE_MAX_FRAMES) {
    framePaths = subsampleEvenly(framePaths, SCENE_MAX_FRAMES);
  }
  if (framePaths.length >= SCENE_MIN_FRAMES) {
    console.log("[frames] strategy: scene-detection-low-threshold, count:", framePaths.length);
    return framePaths;
  }

  await unlinkMatching(sceneFramesDir, /^frame_\d+\.jpg$/i);

  // STEP 3 — evenly spaced (8 midpoints)
  const duration = await getVideoDurationSec(inputPath, isWebm);
  if (duration != null && Number.isFinite(duration) && duration > 0) {
    framePaths = await extractEvenlySpaced(inputPath, sceneFramesDir, duration, isWebm);
    console.log("[frames] strategy: evenly-spaced, duration:", duration);
    if (framePaths.length >= SCENE_MIN_FRAMES) {
      return framePaths;
    }
  }

  // STEP 4 — fixed timestamps
  await unlinkMatching(sceneFramesDir, /^even_\d+\.jpg$/i);
  const fixedTimes = fixedTimestampsForMovement(movementType);
  framePaths = await extractFixedTimestamps(inputPath, sceneFramesDir, fixedTimes, isWebm);
  console.log("[frames] strategy: fixed-timestamps");
  return framePaths;
}

/**
 * @param {string[]} paths
 * @returns {Promise<{ base64: string, mediaType: string }[]>}
 */
async function readFramePayloads(paths) {
  const frames = [];
  for (const p of paths) {
    const jpeg = await fs.readFile(p);
    frames.push({
      base64: jpeg.toString("base64"),
      mediaType: "image/jpeg",
    });
  }
  return frames;
}

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.post("/extract-frames", async (req, res) => {
  if (!authOk(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return res.status(500).json({ error: "Missing Supabase env vars" });
  }

  const { analysisId, storagePath, movementType } = req.body ?? {};

  if (!isSafeId(analysisId) || typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "Invalid analysisId or storagePath" });
  }

  const normalizedPath = storagePath.replace(/^\/+/, "");
  if (normalizedPath.includes("..") || normalizedPath.length > 1024) {
    return res.status(400).json({ error: "Invalid storagePath" });
  }

  const isWebm = isWebmPath(normalizedPath);
  const ext = path.extname(normalizedPath) || ".bin";
  const inputPath = path.join("/tmp", `${analysisId}${ext}`);
  const sceneFramesDir = `/tmp/scene_${analysisId}`;

  /** @type {string[]} */
  let framePaths = [];

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  try {
    const { data: blob, error: downloadError } = await supabase.storage
      .from(STORAGE_BUCKET)
      .download(normalizedPath);

    if (downloadError) {
      const code =
        typeof downloadError.statusCode === "number"
          ? downloadError.statusCode
          : Number(downloadError.statusCode);
      if (code === 404) {
        return res.status(404).json({
          error: "Video not found in storage",
        });
      }
      console.error("[extract-frames] download error:", downloadError.message);
      return res.status(502).json({ error: "Failed to download video" });
    }

    if (!blob) {
      return res.status(404).json({ error: "Video not found in storage" });
    }

    const buf = Buffer.from(await blob.arrayBuffer());
    await fs.writeFile(inputPath, buf);

    framePaths = await extractFramePathsUniversal(
      inputPath,
      analysisId,
      movementType,
      isWebm,
    );

    if (framePaths.length === 0) {
      return res.status(500).json({ error: "No frames extracted" });
    }

    const frames = await readFramePayloads(framePaths);

    return res.json({ frames });
  } catch (err) {
    console.error("[extract-frames]", err);
    return res.status(500).json({ error: "Frame extraction failed" });
  } finally {
    try {
      if (fssync.existsSync(inputPath)) {
        await fs.unlink(inputPath).catch(() => {});
      }
    } catch {
      /* ignore */
    }
    try {
      await fs.rm(sceneFramesDir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }
});

app.listen(PORT, () => {
  console.log(`movement-frames listening on ${PORT}`);
});
