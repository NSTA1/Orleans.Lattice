import assert from "node:assert/strict";
import { test } from "node:test";
import { silentWav, wavDuration } from "../lib/wav.js";

/** A minimal WAV: a RIFF header, an optional extra chunk, fmt, then `samples` frames of silence. */
function wav({ sampleRate = 24000, channels = 1, bitsPerSample = 16, samples = 24000, extra = false } = {}) {
  const blockAlign = channels * (bitsPerSample / 8);
  const data = Buffer.alloc(samples * blockAlign);
  const chunks = [];
  if (extra) {
    const list = Buffer.alloc(8 + 3 + 1);
    list.write("LIST", 0, "ascii");
    list.writeUInt32LE(3, 4);
    chunks.push(list);
  }
  const fmt = Buffer.alloc(8 + 16);
  fmt.write("fmt ", 0, "ascii");
  fmt.writeUInt32LE(16, 4);
  fmt.writeUInt16LE(bitsPerSample === 32 ? 3 : 1, 8);
  fmt.writeUInt16LE(channels, 10);
  fmt.writeUInt32LE(sampleRate, 12);
  fmt.writeUInt32LE(sampleRate * blockAlign, 16);
  fmt.writeUInt16LE(blockAlign, 20);
  fmt.writeUInt16LE(bitsPerSample, 22);
  chunks.push(fmt);
  const header = Buffer.alloc(8);
  header.write("data", 0, "ascii");
  header.writeUInt32LE(data.length, 4);
  chunks.push(header, data);
  const body = Buffer.concat(chunks);
  const riff = Buffer.alloc(12);
  riff.write("RIFF", 0, "ascii");
  riff.writeUInt32LE(4 + body.length, 4);
  riff.write("WAVE", 8, "ascii");
  return Buffer.concat([riff, body]);
}

test("the duration is the data size over the byte rate", () => {
  assert.equal(wavDuration(wav({ samples: 36000 })), 1.5);
  assert.equal(wavDuration(wav({ sampleRate: 48000, channels: 2, bitsPerSample: 32, samples: 12000 })), 0.25);
});

test("chunks before fmt, including an odd-sized one, are skipped", () => {
  assert.equal(wavDuration(wav({ extra: true, samples: 12000 })), 0.5);
});

test("a file that is not a WAV, or has no data, is an error", () => {
  assert.throws(() => wavDuration(Buffer.from("not a wav file at all")), /not a RIFF\/WAVE file/);
  const truncated = wav().subarray(0, 12 + 24);
  assert.throws(() => wavDuration(truncated), /no data chunk/);
});

test("a silent stand-in is a valid WAV of the length asked for, and silent", () => {
  const buffer = silentWav(2.5);
  assert.equal(wavDuration(buffer), 2.5);
  assert.ok(buffer.subarray(44).every((byte) => byte === 0));
  assert.equal(wavDuration(silentWav(1, 48000)), 1);
});
