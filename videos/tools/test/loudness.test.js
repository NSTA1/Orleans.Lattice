import assert from "node:assert/strict";
import { test } from "node:test";
import { DELIVERED_AS, gainFor, LIMITER_CEILING_DB, LOUDNESS_TARGET, masteringFilter, MAX_GAIN_DB, onTarget, parseEbur128 } from "../lib/loudness.js";

// The tail of `ffmpeg -i x.wav -af ebur128=peak=true -f null -`, as FFmpeg prints it.
const summary = (integrated, peak) => `[Parsed_ebur128_0 @ 000001] t: 9.9 TARGET:-23 LUFS M: -20.1 S: -20.4 I: -19.9 LUFS
[Parsed_ebur128_0 @ 000001] Summary:

  Integrated loudness:
    I:         ${integrated} LUFS
    Threshold: -30.1 LUFS

  Loudness range:
    LRA:         3.6 LU
    Threshold: -40.2 LUFS
    LRA low:   -22.1 LUFS
    LRA high:  -18.5 LUFS

  True peak:
    Peak:       ${peak} dBFS
`;

test("the target is the series loudness: -16 LUFS, true peak below -1 dBTP", () => {
  assert.equal(LOUDNESS_TARGET.integrated, -16);
  assert.equal(LOUDNESS_TARGET.truePeakCeiling, -1);
  assert.ok(LIMITER_CEILING_DB < LOUDNESS_TARGET.truePeakCeiling, "the limiter leaves room for inter-sample peaks");
});

test("loudness is measured as delivered: the mono narration on both channels of a stereo track", () => {
  assert.equal(DELIVERED_AS, "pan=stereo|c0=c0|c1=c0");
});

test("the integrated loudness and true peak are read from the summary, not from the running meter", () => {
  assert.deepEqual(parseEbur128(summary("-19.6", "-4.2")), { integrated: -19.6, truePeak: -4.2 });
  assert.deepEqual(parseEbur128(summary("-70.0", "-inf")), { integrated: -70, truePeak: -Infinity });
});

test("output without a full summary is an error, never a guess", () => {
  assert.throws(() => parseEbur128("t: 1.0 M: -20 S: -21 I: -19.9 LUFS"), /no ebur128 summary/);
  assert.throws(() => parseEbur128(summary("-19.6", "-4.2").replace(/True peak:[\s\S]*$/, "")), /no true peak/);
});

test("the gain moves the integrated loudness onto the target, and the limiter, not the gain, deals with peaks", () => {
  assert.equal(gainFor({ integrated: -20.1, truePeak: -0.4 }), 4.1);
  assert.equal(gainFor({ integrated: -12.5, truePeak: -2 }), -3.5, "a loud track is turned down");
  assert.equal(gainFor({ integrated: -60, truePeak: -40 }), MAX_GAIN_DB);
});

test("a silent track has no gain", () => {
  assert.throws(() => gainFor({ integrated: -70, truePeak: -Infinity }), /silent/);
  assert.throws(() => gainFor({ integrated: -Infinity, truePeak: -Infinity }), /silent/);
});

test("mastering is one gain and a limiter that does not renormalise", () => {
  assert.equal(masteringFilter(4.1), "volume=4.1dB,alimiter=limit=0.7499:attack=5:release=50:level=false");
  assert.equal(masteringFilter(-3, -6), "volume=-3dB,alimiter=limit=0.5012:attack=5:release=50:level=false");
});

test("a track is on target within the tolerance and under the ceiling", () => {
  assert.equal(onTarget({ integrated: -16.3, truePeak: -1.6 }), true);
  assert.equal(onTarget({ integrated: -17, truePeak: -1.6 }), false, "too quiet");
  assert.equal(onTarget({ integrated: -16, truePeak: -0.8 }), false, "peaks over the ceiling");
});
