// The series loudness (frame.md, "Audio"): -16 LUFS integrated, with the true
// peak below -1 dBTP, the same in every episode. Kokoro's raw speech measures
// about -20 LUFS with peaks close to full scale, so a gain alone cannot reach
// the target without clipping: the narration is mastered with one gain and a
// peak limiter, then measured again to prove it landed.

/** The target every episode's narration is mastered to. */
export const LOUDNESS_TARGET = Object.freeze({ integrated: -16, truePeakCeiling: -1, tolerance: 0.5 });

/**
 * How the narration is heard: the render places the mono narration on both
 * channels of a stereo track, and loudness sums the channels, so dual mono
 * measures 3 LU louder than the same mono file. Measuring through this filter
 * measures what is delivered, so the delivered track, not the mono master,
 * meets the target.
 */
export const DELIVERED_AS = "pan=stereo|c0=c0|c1=c0";

/**
 * Where the limiter holds sample peaks, in dBFS. It sits below the true-peak
 * ceiling because a true peak can fall between samples and read higher.
 */
export const LIMITER_CEILING_DB = -2.5;

/** The most gain mastering will apply: anything quieter is not a narration track. */
export const MAX_GAIN_DB = 20;

// ebur128 reports the floor of its absolute gate for a track with nothing
// above it: silence, as far as loudness is concerned.
const SILENT_LUFS = -70;

/**
 * Reads the summary that FFmpeg's ebur128 filter (run with peak=true) prints
 * at the end of its output: the integrated loudness in LUFS and the true peak
 * in dBFS.
 */
export function parseEbur128(stderr) {
  if (!stderr.includes("Summary:")) {
    throw new Error("loudness: FFmpeg printed no ebur128 summary");
  }
  const summary = stderr.slice(stderr.lastIndexOf("Summary:"));
  const integrated = /Integrated loudness:\s*I:\s*(-?[\d.]+|-inf)\s*LUFS/.exec(summary);
  const truePeak = /True peak:\s*Peak:\s*(-?[\d.]+|-inf)\s*dBFS/.exec(summary);
  if (!integrated || !truePeak) {
    throw new Error("loudness: the ebur128 summary has no integrated loudness or no true peak (run it with peak=true)");
  }
  const value = (text) => (text === "-inf" ? -Infinity : Number(text));
  return { integrated: value(integrated[1]), truePeak: value(truePeak[1]) };
}

/** The gain, in dB, that moves a track's integrated loudness onto the target. */
export function gainFor({ integrated }, target = LOUDNESS_TARGET) {
  if (!Number.isFinite(integrated) || integrated <= SILENT_LUFS) {
    throw new Error("loudness: the track is silent, so there is nothing to bring to the target");
  }
  return Math.round(Math.min(target.integrated - integrated, MAX_GAIN_DB) * 100) / 100;
}

/** The FFmpeg filter that masters a track: one gain, then a peak limiter at the ceiling. */
export function masteringFilter(gainDb, ceilingDb = LIMITER_CEILING_DB) {
  const limit = Math.round(10 ** (ceilingDb / 20) * 10000) / 10000;
  // level=false keeps the limiter from renormalising its output; the gain is ours.
  return `volume=${gainDb}dB,alimiter=limit=${limit}:attack=5:release=50:level=false`;
}

/** Whether a measured track is on the target: within tolerance of its loudness, and under its true-peak ceiling. */
export function onTarget({ integrated, truePeak }, target = LOUDNESS_TARGET) {
  return Math.abs(integrated - target.integrated) <= target.tolerance && truePeak <= target.truePeakCeiling;
}
