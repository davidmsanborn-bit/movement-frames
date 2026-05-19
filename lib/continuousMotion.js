/**
 * Continuous (motion-aware) temporal windows for cut-less game film.
 * Used by detectSceneWindows when scene-cut coverage is insufficient.
 */

const { spawn } = require("child_process");

const YDIF_DIVISOR = 5.0;

function parseEnvPositiveInt(raw, fallback, min, max) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  const t = Math.trunc(n);
  if (t < min || t > max) return fallback;
  return t;
}

function parseEnvFloat(raw, fallback, min, max) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  if (n < min || n > max) return fallback;
  return n;
}

function parseContinuousEnv() {
  return {
    combinedTimeoutMs: parseEnvPositiveInt(
      process.env.FRAMES_COMBINED_TIMEOUT_MS,
      185_000,
      1,
      600_000,
    ),
    coverageFloorPct: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_COVERAGE_FLOOR_PCT,
      55,
      1,
      100,
    ),
    minWindows: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_MIN_WINDOWS,
      12,
      1,
      500,
    ),
    minDurationMin: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_MIN_DURATION_MIN,
      20,
      1,
      240,
    ),
    windowSec: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_WINDOW_SEC,
      30,
      5,
      120,
    ),
    maxWindows: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_MAX_WINDOWS,
      220,
      1,
      500,
    ),
    targetCoveragePct: parseEnvPositiveInt(
      process.env.FRAMES_CONTINUOUS_TARGET_COVERAGE_PCT,
      90,
      1,
      100,
    ),
    staticIntensity: parseEnvFloat(
      process.env.FRAMES_CONTINUOUS_STATIC_INTENSITY,
      0.35,
      0,
      1,
    ),
  };
}

/**
 * Union length of [start,end) windows in seconds (merge overlaps).
 */
function computeTimelineUnionSec(windows) {
  if (!windows.length) return 0;
  const intervals = windows
    .map((w) => [Number(w.start_sec ?? w.start), Number(w.end_sec ?? w.end)])
    .filter(([s, e]) => Number.isFinite(s) && Number.isFinite(e) && e > s)
    .sort((a, b) => a[0] - b[0]);
  if (!intervals.length) return 0;

  let union = 0;
  let curStart = intervals[0][0];
  let curEnd = intervals[0][1];
  for (let i = 1; i < intervals.length; i++) {
    const [s, e] = intervals[i];
    if (s <= curEnd) {
      curEnd = Math.max(curEnd, e);
    } else {
      union += curEnd - curStart;
      curStart = s;
      curEnd = e;
    }
  }
  union += curEnd - curStart;
  return union;
}

function computeTimelineCoveragePct(windows, durationSec) {
  if (!durationSec || durationSec <= 0) return 0;
  return (computeTimelineUnionSec(windows) / durationSec) * 100;
}

function evaluateContinuousTrigger(windows, durationSec, env) {
  const windowCount = windows.length;
  const coveragePct = computeTimelineCoveragePct(windows, durationSec);
  const minDurationSec = env.minDurationMin * 60;
  const reasons = [];
  if (coveragePct < env.coverageFloorPct) {
    reasons.push("coverage_below_floor");
  }
  if (durationSec >= minDurationSec && windowCount < env.minWindows) {
    reasons.push("long_game_sparse_windows");
  }
  return {
    shouldTrigger: reasons.length > 0,
    triggerReason: reasons.join("|") || null,
    sceneCutWindowCount: windowCount,
    sceneCutCoveragePct: Math.round(coveragePct * 100) / 100,
  };
}

/**
 * Contiguous fixed windows from t=0; count from target/floor coverage, capped.
 */
function planContinuousWindows(durationSec, env) {
  const W = env.windowSec;
  const nTarget = Math.ceil((durationSec * env.targetCoveragePct) / 100 / W);
  const nFloor = Math.ceil((durationSec * env.coverageFloorPct) / 100 / W);
  let n = Math.min(env.maxWindows, Math.max(nTarget, nFloor, 1));

  const build = (count) => {
    const out = [];
    for (let i = 0; i < count; i++) {
      const start = i * W;
      if (start >= durationSec) break;
      const end = Math.min(durationSec, start + W);
      if (end - start < 0.5) break;
      out.push({ start, end });
    }
    return out;
  };

  let slots = build(n);
  let coveragePct = computeTimelineCoveragePct(
    slots.map((s) => ({ start_sec: s.start, end_sec: s.end })),
    durationSec,
  );

  while (coveragePct < env.coverageFloorPct && n < env.maxWindows) {
    n += 1;
    slots = build(n);
    coveragePct = computeTimelineCoveragePct(
      slots.map((s) => ({ start_sec: s.start, end_sec: s.end })),
      durationSec,
    );
  }

  const cappedByMax = n >= env.maxWindows && coveragePct < env.coverageFloorPct;

  return {
    slots,
    windowCount: slots.length,
    emittedCoveragePct: Math.round(coveragePct * 100) / 100,
    cappedByMax,
  };
}

function normalizeYdifIntensity(ydifValues, staticFallback, useP90 = true) {
  if (!ydifValues.length) return staticFallback;
  const sorted = [...ydifValues].sort((a, b) => a - b);
  const stat = useP90
    ? sorted[Math.floor(sorted.length * 0.9)] ?? sorted[sorted.length - 1]
    : sorted.reduce((a, b) => a + b, 0) / sorted.length;
  return Math.max(0, Math.min(1, stat / YDIF_DIVISOR));
}

/**
 * Parse pts_time + YDIF pairs from combined-pass stderr (signalstats branch).
 */
function parseYdifSamplesFromStderr(stderr) {
  const samples = [];
  let currentPts = null;
  const lines = stderr.split("\n");
  for (const line of lines) {
    const ptsM = line.match(/pts_time:([0-9.]+)/);
    if (ptsM) {
      const t = parseFloat(ptsM[1]);
      if (!isNaN(t)) currentPts = t;
    }
    const ydifM = line.match(/lavfi\.signalstats\.YDIF=([0-9.]+)/);
    if (ydifM && currentPts !== null) {
      const v = parseFloat(ydifM[1]);
      if (!isNaN(v)) samples.push({ t: currentPts, ydif: v });
    }
  }
  return samples;
}

function aggregateYdifForWindow(samples, startSec, endSec) {
  const vals = [];
  for (const s of samples) {
    if (s.t >= startSec && s.t < endSec) vals.push(s.ydif);
  }
  return vals;
}

/**
 * One ffmpeg child: scene branch nullsink + signalstats @ fps=2.
 */
function runCombinedSignalstatsPass(ffmpegPath, inputPath, timeoutMs) {
  const filterComplex =
    "[0:v]split=2[V0][V1];" +
    "[V0]select='gt(scene\\,0.08)',showinfo,nullsink;" +
    "[V1]fps=2,scale=320:180,signalstats,metadata=print,format=yuv420p[out]";

  return new Promise((resolve) => {
    const t0 = Date.now();
    let stderr = "";
    let timedOut = false;
    let settled = false;

    const proc = spawn(
      ffmpegPath,
      [
        "-hide_banner",
        "-loglevel",
        "info",
        "-i",
        inputPath,
        "-an",
        "-filter_complex",
        filterComplex,
        "-map",
        "[out]",
        "-f",
        "null",
        "-",
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );

    const timer = setTimeout(() => {
      timedOut = true;
      try {
        proc.kill("SIGKILL");
      } catch {
        /* */
      }
    }, timeoutMs);

    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 80_000_000) {
        stderr = "...[truncated]..." + stderr.slice(-40_000_000);
      }
    });

    proc.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({
        ok: false,
        timedOut: false,
        wallMs: Date.now() - t0,
        samples: [],
        error: err instanceof Error ? err.message : String(err),
      });
    });

    proc.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const wallMs = Date.now() - t0;
      if (timedOut) {
        resolve({
          ok: false,
          timedOut: true,
          wallMs,
          samples: [],
          error: "COMBINED_FFMPEG_TIMEOUT",
        });
        return;
      }
      if (code !== 0) {
        resolve({
          ok: false,
          timedOut: false,
          wallMs,
          samples: [],
          error: `combined ffmpeg exited ${code}`,
        });
        return;
      }
      resolve({
        ok: true,
        timedOut: false,
        wallMs,
        samples: parseYdifSamplesFromStderr(stderr),
        error: null,
      });
    });
  });
}

function buildContinuousSceneRecords(slots, samples, path, env) {
  const useMotion = path === "motion";
  const source = useMotion
    ? "continuous_motion"
    : "continuous_static_fallback";
  const confidence = useMotion ? 0.6 : 0.5;
  const intensitySource = useMotion ? "ydif_p90" : "static";

  const scenes = [];
  for (const slot of slots) {
    let intensityScore = env.staticIntensity;
    let sceneType = useMotion ? "motion" : "uniform";

    if (useMotion) {
      const ydifVals = aggregateYdifForWindow(samples, slot.start, slot.end);
      intensityScore =
        ydifVals.length > 0
          ? normalizeYdifIntensity(ydifVals, env.staticIntensity, true)
          : env.staticIntensity;
    }

    const span = slot.end - slot.start;
    scenes.push({
      start_sec: Math.round(slot.start * 10) / 10,
      end_sec: Math.round(slot.end * 10) / 10,
      intensity_score: Math.round(intensityScore * 100) / 100,
      scene_type: sceneType,
      confidence,
      source,
      duration_sec: Math.round(span * 10) / 10,
    });
  }

  return { scenes, intensitySource };
}

function logDetectScenesMotion(payload) {
  console.log(
    JSON.stringify({
      type: "detect_scenes_motion",
      ...payload,
    }),
  );
}

/**
 * Run continuous path: combined ffmpeg (optional) → fixed windows → scenes[].
 */
async function runContinuousMotionPath(
  ffmpegPath,
  workingPath,
  gameId,
  durationSec,
  triggerMeta,
) {
  const env = parseContinuousEnv();
  const plan = planContinuousWindows(durationSec, env);

  const combined = await runCombinedSignalstatsPass(
    ffmpegPath,
    workingPath,
    env.combinedTimeoutMs,
  );

  let path;
  let scenes;
  let intensitySource;
  if (combined.ok && combined.samples.length > 0) {
    path = "motion";
    const built = buildContinuousSceneRecords(
      plan.slots,
      combined.samples,
      path,
      env,
    );
    scenes = built.scenes;
    intensitySource = built.intensitySource;
  } else {
    path = "static_fallback";
    const built = buildContinuousSceneRecords(plan.slots, [], path, env);
    scenes = built.scenes;
    intensitySource = built.intensitySource;
  }

  const emittedCoveragePct = computeTimelineCoveragePct(scenes, durationSec);

  logDetectScenesMotion({
    game_id: gameId,
    duration_sec: Math.round(durationSec),
    scene_cut_window_count: triggerMeta.sceneCutWindowCount,
    scene_cut_coverage_pct: triggerMeta.sceneCutCoveragePct,
    trigger_reason: triggerMeta.triggerReason,
    path,
    combined_ffmpeg_ms: combined.wallMs,
    timed_out: combined.timedOut,
    emitted_window_count: scenes.length,
    emitted_coverage_pct: Math.round(emittedCoveragePct * 100) / 100,
    intensity_source: intensitySource,
    capped_by_max_windows: plan.cappedByMax,
    combined_error: combined.error || undefined,
  });

  return scenes;
}

module.exports = {
  parseContinuousEnv,
  computeTimelineCoveragePct,
  evaluateContinuousTrigger,
  planContinuousWindows,
  runCombinedSignalstatsPass,
  buildContinuousSceneRecords,
  runContinuousMotionPath,
  parseYdifSamplesFromStderr,
};
