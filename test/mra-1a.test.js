/**
 * MR-A.1a local smoke test for /extract-game-film-crops.
 *
 * Run:
 *   SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... FRAMES_SERVICE_SECRET=... node test/mra-1a.test.js
 */

const fs = require("fs");
const path = require("path");

const BASE_URL = process.env.FRAMES_BASE_URL || "http://localhost:3000";
const FRAMES_SERVICE_SECRET = process.env.FRAMES_SERVICE_SECRET || "";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  assert(process.env.SUPABASE_URL, "SUPABASE_URL is required");
  assert(
    process.env.SUPABASE_SERVICE_ROLE_KEY,
    "SUPABASE_SERVICE_ROLE_KEY is required",
  );
  assert(FRAMES_SERVICE_SECRET, "FRAMES_SERVICE_SECRET is required");

  const payload = {
    gameId: "6dfea380-b209-4963-9973-550b6679d449",
    storagePath:
      "f4dc4e68-a829-499f-943c-09fb2859f9c2/6dfea380-b209-4963-9973-550b6679d449/film.mp4",
    crops: [
      {
        track_id: 168,
        timestamp: 12.0,
        bbox: [432, 188, 548, 576],
        center_x: null,
        center_y: null,
      },
      {
        track_id: 168,
        timestamp: 24.0,
        bbox: [440, 196, 560, 590],
        center_x: null,
        center_y: null,
      },
      {
        track_id: 65,
        timestamp: 36.0,
        bbox: null,
        center_x: 0.52,
        center_y: 0.58,
      },
      {
        track_id: 65,
        timestamp: 48.0,
        bbox: null,
        center_x: 0.46,
        center_y: 0.55,
      },
    ],
  };

  const startedAt = Date.now();
  const res = await fetch(`${BASE_URL}/extract-game-film-crops`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${FRAMES_SERVICE_SECRET}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const elapsedMs = Date.now() - startedAt;
  const text = await res.text();
  let body = null;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Error(`Expected JSON response, got: ${text.slice(0, 500)}`);
  }

  assert(res.status === 200, `Expected 200, got ${res.status}: ${text}`);
  assert(
    Number(body.crops_processed) === 4,
    `Expected crops_processed=4, got ${body.crops_processed}`,
  );
  assert(Array.isArray(body.crops), "Expected crops array");
  assert(body.crops.length === 4, `Expected 4 crops, got ${body.crops.length}`);

  const outPaths = [];
  for (let i = 0; i < body.crops.length; i++) {
    const crop = body.crops[i];
    assert(
      typeof crop.image_base64 === "string" && crop.image_base64.length > 0,
      `Crop ${i} image_base64 is empty`,
    );
    const outPath = path.join("/tmp", `mra-1a-test-crop-${i}.jpg`);
    fs.writeFileSync(outPath, Buffer.from(crop.image_base64, "base64"));
    outPaths.push(outPath);
  }

  console.log("[mra-1a-test] status=200 ok");
  console.log("[mra-1a-test] request_elapsed_ms=", elapsedMs);
  console.log("[mra-1a-test] crops_processed=", body.crops_processed);
  console.log("[mra-1a-test] crops_skipped=", body.crops_skipped);
  console.log("[mra-1a-test] video_metadata=", JSON.stringify(body.video_metadata));
  console.log("[mra-1a-test] output_files=", outPaths.join(","));
}

main().catch((err) => {
  console.error("[mra-1a-test] failed:", err.message);
  process.exit(1);
});
