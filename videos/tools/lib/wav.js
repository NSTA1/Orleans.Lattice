/**
 * A WAV file of silence: 16-bit PCM, mono, `seconds` long. It stands in for
 * narration that has not been generated on this machine, so an episode's
 * pictures and structure can still be checked.
 */
export function silentWav(seconds, sampleRate = 24000) {
  const samples = Math.round(seconds * sampleRate);
  const data = samples * 2;
  const buffer = Buffer.alloc(44 + data);
  buffer.write("RIFF", 0, "ascii");
  buffer.writeUInt32LE(36 + data, 4);
  buffer.write("WAVE", 8, "ascii");
  buffer.write("fmt ", 12, "ascii");
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20);
  buffer.writeUInt16LE(1, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(sampleRate * 2, 28);
  buffer.writeUInt16LE(2, 32);
  buffer.writeUInt16LE(16, 34);
  buffer.write("data", 36, "ascii");
  buffer.writeUInt32LE(data, 40);
  return buffer;
}

/**
 * The duration in seconds of a WAV file, read from its RIFF header: the size
 * of its data chunk over the byte rate its fmt chunk declares. Works for any
 * sample format, since the byte rate already accounts for it.
 */
export function wavDuration(buffer) {
  if (buffer.length < 12 || buffer.toString("ascii", 0, 4) !== "RIFF" || buffer.toString("ascii", 8, 12) !== "WAVE") {
    throw new Error("wav: not a RIFF/WAVE file");
  }
  let byteRate = null;
  let offset = 12;
  while (offset + 8 <= buffer.length) {
    const id = buffer.toString("ascii", offset, offset + 4);
    const size = buffer.readUInt32LE(offset + 4);
    const body = offset + 8;
    if (id === "fmt ") {
      byteRate = buffer.readUInt32LE(body + 8);
    } else if (id === "data") {
      if (!byteRate) {
        throw new Error("wav: the data chunk comes before a usable fmt chunk");
      }
      // A streamed file can declare a larger size than it holds; trust the bytes.
      return Math.min(size, buffer.length - body) / byteRate;
    }
    offset = body + size + (size % 2);
  }
  throw new Error("wav: no data chunk");
}
