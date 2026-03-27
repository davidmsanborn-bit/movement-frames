const express = require("express");
const { createClient } = require("@supabase/supabase-js");
const fs = require("fs/promises");
const path = require("path");
const { execFile } = require("child_process");
const { promisify } = require("util");

const execFileAsync = promisify(execFile);

const PORT = Number(process.env.PORT) || 3001;
const SERVICE_SECRET = process.env.SERVICE_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "videos";

/** Default `ffmpeg` on Linux/Railway; set e.g. `/opt/homebrew/bin/ffmpeg` locally if needed */
const FFMPEG = process.env.FFMPEG_PATH || "ffmpeg";

/** Normalize high-fps / slo-mo (e.g. 120/240fps) to 30fps before scale so seeks and extracts behave consistently */
const VF_EXTRACT = "fps=30,scale=800:-1";

const SQUAT_FRAME_SECONDS = [1, 2, 3];
const SHOOTING_FRAME_COUNT = 8;
const SCENE_MIN_FRAMES = 4;
const SCENE_MAX_FRAMES = 10;

/** METHOD 3 — when duration cannot be determined (e.g. broken webm metadata) */
const FALLBACK_SHOOTING_SEEK_SEC = [1, 2, 3, 4, 5, 6, 7, 8];

function isSafeId(value) {
  return typeof value === "string" && /^[a-zA-Z0-9._-]+$/.test(value) && value.length <= 256;
}

function authOk(req) {
  if (!SERVICE_SECRET) return false;
  const h = req.headers.authorization;
  if (typeof h !== "string") return false;
  return h === SERVICE_SECRET || h === `Bearer ${SERVICE_SECRET}`;
}

/** @param {unknown} movementType */
function resolveStrategy(movementType) {
  if (movementType == null) return "squat";
  const mt = String(movementType).toLowerCase().trim();
  if (mt === "shooting" || mt === "basketball") return "shooting";
  return "squat";
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

/** @param {number} durationSec */
function shootingFrameTimesSec(durationSec) {
  const n = SHOOTING_FRAME_COUNT;
  const times = [];
  for (let i = 0; i < n; i++) {
    times.push(((i + 0.5) / n) * durationSec);
  }
  return times;
}

/**
 * List /tmp/{analysisId}-scene-NNN.jpg from scene-detect pass, sorted by frame number.
 * @param {string} analysisId
 * @returns {Promise<string[]>}
 */
async function listSceneFramePaths(analysisId) {
  const dir = "/tmp";
  const prefix = `${analysisId}-scene-`;
  let entries;
  try {
    entries = await fs.readdir(dir);
  } catch {
    return [];
  }
  const paths = entries
    .filter((f) => f.startsWith(prefix) && f.endsWith(".jpg"))
    .map((f) => {
      const m = f.match(/-scene-(\d+)\.jpg$/);
      const n = m ? parseInt(m[1], 10) : NaN;
      return { n, p: path.join(dir, f) };
    })
    .filter((x) => Number.isFinite(x.n))
    .sort((a, b) => a.n - b.n)
    .map((x) => x.p);
  return paths;
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

  const strategy = resolveStrategy(movementType);
  const isWebm = isWebmPath(normalizedPath);
  const ext = path.extname(normalizedPath) || ".bin";
  const inputPath = path.join("/tmp", `${analysisId}${ext}`);

  /** @type {string[]} */
  let framePaths = [];
  /** @type {number[]} */
  let timesSec = [];

  if (strategy === "squat") {
    framePaths = SQUAT_FRAME_SECONDS.map((sec) =>
      path.join("/tmp", `${analysisId}-frame-${sec}s.jpg`),
    );
    timesSec = [...SQUAT_FRAME_SECONDS];
  }

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

    const ffmpegBase = [
      "-vframes",
      "1",
      "-vf",
      VF_EXTRACT,
      "-pix_fmt",
      "yuvj420p",
      "-q:v",
      "2",
    ];

    if (strategy === "shooting") {
      const sceneOutPattern = path.join("/tmp", `${analysisId}-scene-%03d.jpg`);
      const sceneVf = "select=gt(scene\\,0.15),fps=30,scale=800:-1";

      try {
        await execFileAsync(
          FFMPEG,
          [
            ...inputPrefixFlags(isWebm),
            "-i",
            inputPath,
            "-vf",
            sceneVf,
            "-vsync",
            "vfr",
            "-pix_fmt",
            "yuvj420p",
            "-q:v",
            "2",
            sceneOutPattern,
          ],
          { maxBuffer: 50 * 1024 * 1024 },
        );
      } catch (err) {
        console.error("[extract-frames] scene detection pass failed:", err.message);
      }

      let scenePaths = await listSceneFramePaths(analysisId);
      if (scenePaths.length > SCENE_MAX_FRAMES) {
        scenePaths = scenePaths.slice(0, SCENE_MAX_FRAMES);
      }

      if (scenePaths.length >= SCENE_MIN_FRAMES) {
        framePaths = scenePaths;
      } else {
        const durationSec = await getVideoDurationSec(inputPath, isWebm);
        if (durationSec == null || !Number.isFinite(durationSec) || durationSec <= 0) {
          timesSec = [...FALLBACK_SHOOTING_SEEK_SEC];
          console.log("[frames] duration method:", "fixed-fallback-shooting", timesSec.join(","));
        } else {
          timesSec = shootingFrameTimesSec(durationSec);
        }
        framePaths = Array.from({ length: SHOOTING_FRAME_COUNT }, (_, i) =>
          path.join("/tmp", `${analysisId}-shooting-${i}.jpg`),
        );
        for (let i = 0; i < framePaths.length; i++) {
          await execFileAsync(FFMPEG, [
            "-ss",
            String(timesSec[i]),
            ...inputPrefixFlags(isWebm),
            "-i",
            inputPath,
            ...ffmpegBase,
            framePaths[i],
          ]);
        }
      }
    } else {
      for (let i = 0; i < framePaths.length; i++) {
        const t = timesSec[i];
        const ss = `00:00:${String(t).padStart(2, "0")}`;
        await execFileAsync(FFMPEG, [
          "-ss",
          ss,
          ...inputPrefixFlags(isWebm),
          "-i",
          inputPath,
          ...ffmpegBase,
          framePaths[i],
        ]);
      }
    }

    const frames = await readFramePayloads(framePaths);

    return res.json({ frames });
  } catch (err) {
    console.error("[extract-frames]", err);
    return res.status(500).json({ error: "Frame extraction failed" });
  } finally {
    const toRemove = new Set([inputPath, ...framePaths]);
    try {
      const dir = "/tmp";
      const prefix = `${analysisId}-scene-`;
      const entries = await fs.readdir(dir);
      for (const f of entries) {
        if (f.startsWith(prefix) && f.endsWith(".jpg")) {
          toRemove.add(path.join(dir, f));
        }
      }
      for (let i = 0; i < SHOOTING_FRAME_COUNT; i++) {
        toRemove.add(path.join("/tmp", `${analysisId}-shooting-${i}.jpg`));
      }
    } catch {
      /* ignore */
    }
    await Promise.all(
      [...toRemove].map((p) =>
        fs.unlink(p).catch(() => {
          /* ignore */
        }),
      ),
    );
  }
});

app.listen(PORT, () => {
  console.log(`movement-frames listening on ${PORT}`);
});
