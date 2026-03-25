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

const FRAME_SECONDS = [1, 2, 3];

function isSafeId(value) {
  return typeof value === "string" && /^[a-zA-Z0-9._-]+$/.test(value) && value.length <= 256;
}

function authOk(req) {
  if (!SERVICE_SECRET) return false;
  const h = req.headers.authorization;
  if (typeof h !== "string") return false;
  return h === SERVICE_SECRET || h === `Bearer ${SERVICE_SECRET}`;
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

  const { analysisId, storagePath } = req.body ?? {};

  if (!isSafeId(analysisId) || typeof storagePath !== "string" || !storagePath.trim()) {
    return res.status(400).json({ error: "Invalid analysisId or storagePath" });
  }

  const normalizedPath = storagePath.replace(/^\/+/, "");
  if (normalizedPath.includes("..") || normalizedPath.length > 1024) {
    return res.status(400).json({ error: "Invalid storagePath" });
  }

  const inputPath = path.join("/tmp", `${analysisId}.mov`);
  const framePaths = FRAME_SECONDS.map(
    (sec) => path.join("/tmp", `${analysisId}-frame-${sec}s.jpg`),
  );

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

    for (let i = 0; i < FRAME_SECONDS.length; i++) {
      const sec = FRAME_SECONDS[i];
      const ss = `00:00:${String(sec).padStart(2, "0")}`;
      const out = framePaths[i];
      await execFileAsync("ffmpeg", [
        "-ss",
        ss,
        "-i",
        inputPath,
        "-vframes",
        "1",
        "-vf",
        "scale=800:-1",
        "-pix_fmt",
        "yuvj420p",
        "-q:v",
        "2",
        out,
      ]);
    }

    const frames = [];
    for (const p of framePaths) {
      const jpeg = await fs.readFile(p);
      frames.push({
        base64: jpeg.toString("base64"),
        mediaType: "image/jpeg",
      });
    }

    return res.json({ frames });
  } catch (err) {
    console.error("[extract-frames]", err);
    return res.status(500).json({ error: "Frame extraction failed" });
  } finally {
    const toRemove = [inputPath, ...framePaths];
    await Promise.all(
      toRemove.map((p) =>
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
