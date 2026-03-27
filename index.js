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

const SQUAT_FRAME_SECONDS = [1, 2, 3];
const SHOOTING_FRAME_COUNT = 8;
const SCENE_MIN_FRAMES = 4;
const SCENE_MAX_FRAMES = 10;

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

/**
 * ffmpeg -i prints Duration to stderr and exits non-zero when no output is given.
 * @param {string} inputPath
 * @returns {Promise<number | null>}
 */
async function getVideoDurationSec(inputPath) {
  let combined = "";
  try {
    await execFileAsync(FFMPEG, ["-i", inputPath], { maxBuffer: 10 * 1024 * 1024 });
  } catch (err) {
    const stderr = err.stderr != null ? Buffer.from(err.stderr).toString() : "";
    const stdout = err.stdout != null ? Buffer.from(err.stdout).toString() : "";
    combined = stderr + stdout;
  }
  const m = combined.match(/Duration:\s*(\d{1,2}):(\d{2}):(\d{2}\.\d+)/);
  if (!m) return null;
  const h = parseInt(m[1], 10);
  const min = parseInt(m[2], 10);
  const sec = parseFloat(m[3]);
  return h * 3600 + min * 60 + sec;
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

  const inputPath = path.join("/tmp", `${analysisId}.mov`);

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
      "scale=800:-1",
      "-pix_fmt",
      "yuvj420p",
      "-q:v",
      "2",
    ];

    if (strategy === "shooting") {
      const sceneOutPattern = path.join("/tmp", `${analysisId}-scene-%03d.jpg`);
      const sceneVf = "select=gt(scene\\,0.15),scale=800:-1";

      try {
        await execFileAsync(
          FFMPEG,
          [
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
        const durationSec = await getVideoDurationSec(inputPath);
        if (durationSec == null || !Number.isFinite(durationSec) || durationSec <= 0) {
          return res.status(422).json({ error: "Could not read video duration" });
        }
        timesSec = shootingFrameTimesSec(durationSec);
        framePaths = Array.from({ length: SHOOTING_FRAME_COUNT }, (_, i) =>
          path.join("/tmp", `${analysisId}-shooting-${i}.jpg`),
        );
        for (let i = 0; i < framePaths.length; i++) {
          await execFileAsync(FFMPEG, [
            "-ss",
            String(timesSec[i]),
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
        await execFileAsync(FFMPEG, ["-ss", ss, "-i", inputPath, ...ffmpegBase, framePaths[i]]);
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
