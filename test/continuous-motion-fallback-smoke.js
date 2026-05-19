/**
 * Smoke: FRAMES_COMBINED_TIMEOUT_MS=1 forces static_fallback (combined pass killed).
 * Run: FRAMES_COMBINED_TIMEOUT_MS=1 node test/continuous-motion-fallback-smoke.js
 */
/* eslint-disable no-console */
const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFile, spawn } = require("child_process");
const { promisify } = require("util");
const {
  evaluateContinuousTrigger,
  planContinuousWindows,
  runCombinedSignalstatsPass,
  buildContinuousSceneRecords,
  parseContinuousEnv,
  computeTimelineCoveragePct,
} = require("../lib/continuousMotion");

const execFileAsync = promisify(execFile);

const FFMPEG_PATH =
  process.env.FFMPEG_PATH ||
  (process.platform === "darwin"
    ? "/opt/homebrew/bin/ffmpeg"
    : "/usr/bin/ffmpeg");

async function synthMp4(seconds = 8) {
  const out = path.join(os.tmpdir(), `continuous_smoke_${Date.now()}.mp4`);
  await execFileAsync(FFMPEG_PATH, [
    "-hide_banner",
    "-y",
    "-f",
    "lavfi",
    "-i",
    `testsrc2=d=${seconds}:s=640x360:r=30`,
    "-c:v",
    "libx264",
    "-preset",
    "ultrafast",
    "-pix_fmt",
    "yuv420p",
    out,
  ]);
  return out;
}

function assert(cond, msg) {
  if (!cond) {
    console.error("ASSERT FAIL:", msg);
    process.exit(1);
  }
}

async function main() {
  process.env.FRAMES_COMBINED_TIMEOUT_MS = "1";
  const env = parseContinuousEnv();
  assert(env.combinedTimeoutMs === 1, "timeout env should be 1ms");

  const trigger = evaluateContinuousTrigger(
    [
      { start_sec: 0, end_sec: 15 },
      { start_sec: 200, end_sec: 215 },
    ],
    3000,
    env,
  );
  assert(trigger.shouldTrigger, "Campus-like sparse scene-cut should trigger");
  assert(
    trigger.triggerReason.includes("coverage_below_floor"),
    "coverage reason present",
  );
  assert(
    trigger.triggerReason.includes("long_game_sparse_windows"),
    "sparse windows reason present",
  );

  const plan = planContinuousWindows(3000, env);
  assert(plan.windowCount === 90, `expected 90 windows at 90% target, got ${plan.windowCount}`);
  assert(
    plan.emittedCoveragePct >= 89.9 && plan.emittedCoveragePct <= 90.1,
    `expected ~90% coverage, got ${plan.emittedCoveragePct}`,
  );

  let tmpPath = null;
  try {
    tmpPath = await synthMp4(8);
    const combined = await runCombinedSignalstatsPass(FFMPEG_PATH, tmpPath, 1);
    assert(combined.timedOut === true, "combined pass should time out at 1ms");
    assert(combined.ok === false, "combined pass should not succeed");

    const built = buildContinuousSceneRecords(
      planContinuousWindows(8, env).slots,
      [],
      "static_fallback",
      env,
    );
    assert(
      built.scenes.length >= 1,
      "should emit at least one continuous window",
    );
    assert(
      built.scenes.every((s) => s.source === "continuous_static_fallback"),
      "all windows must be continuous_static_fallback",
    );
    assert(
      built.scenes.every((s) => s.scene_type === "uniform"),
      "static fallback scene_type must be uniform",
    );
    assert(
      built.scenes.every((s) => s.intensity_score === 0.35),
      "static intensity default 0.35",
    );

    const cov = computeTimelineCoveragePct(built.scenes, 8);
    console.log(
      JSON.stringify({
        type: "continuous_motion_smoke_ok",
        timed_out: combined.timedOut,
        combined_wall_ms: combined.wallMs,
        combined_error: combined.error,
        path: "static_fallback",
        emitted_window_count: built.scenes.length,
        emitted_coverage_pct: Math.round(cov * 100) / 100,
        sample_window: built.scenes[0],
      }),
    );
    console.log("continuous-motion-fallback-smoke: PASS");
  } finally {
    if (tmpPath && fs.existsSync(tmpPath)) {
      try {
        fs.unlinkSync(tmpPath);
      } catch {
        /* */
      }
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
