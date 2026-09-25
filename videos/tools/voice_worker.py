"""The series voice: speaks narration cues with Chatterbox, cloned from the
reference clip, and transcribes each clip with local speech recognisers so
that tools/narrate.js can check it says what the script says.

tools/narrate.js starts this once per run, because loading the models takes a
minute, and talks to it a line at a time:

  stdin   one JSON request per line:
            {"id": 1, "text": "...", "seed": 123, "output": "clip.wav"}   speak a cue
            {"id": 2, "analyse": "clip.wav", "output": "repaired.wav"}    inspect a clip made before
  stdout  one JSON message per line, prefixed with MARK:
            {"ready": true, "sampleRate": 24000, "versions": {...}}   once, when loaded
            {"id": 1, "ok": true, "seconds": 3.2, "rawSeconds": 3.6,
             "transcripts": {"base.en": "..."}, "repaired": null, "artefacts": [],
             "generateSeconds": 41.0}
            {"id": 1, "ok": false, "error": "..."}

Every clip is inspected for sounds that are not speech, which recognisers
ignore (see inspect): a squeal or burst after the last word is cut off, and
one before it is reported, so the clip can be made again.

Everything the libraries print, progress bars included, goes to stderr, so it
can never be mistaken for a reply. Runs on the CPU, with the model files
pinned to one revision of the model repository.

  python tools/voice_worker.py --reference voice/reference.wav --revision <sha> ...
"""
import argparse
import json
import os
import sys
import time

MARK = "@@voice "
FILES = ["ve.safetensors", "t3_cfg.safetensors", "s3gen.safetensors", "tokenizer.json", "conds.pt"]


def parse():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default="ResembleAI/chatterbox")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--exaggeration", type=float, required=True)
    parser.add_argument("--cfg-weight", type=float, required=True)
    parser.add_argument("--temperature", type=float, required=True)
    parser.add_argument("--trim-threshold-db", type=float, default=-50.0)
    parser.add_argument("--trim-pad-before", type=float, default=0.05)
    parser.add_argument("--trim-pad-after", type=float, default=0.15)
    parser.add_argument("--recognisers", default="base.en,small.en")
    parser.add_argument("--hotwords", default="")
    parser.add_argument("--threads", type=int, default=8)
    return parser.parse_args()


def trim(audio, rate, threshold_db, pad_before, pad_after):
    """Cuts the silence before the first and after the last sound above the threshold, keeping a little of each."""
    import numpy as np

    loud = np.flatnonzero(np.abs(audio) > 10 ** (threshold_db / 20))
    if loud.size == 0:
        return audio
    start = max(0, int(loud[0]) - int(pad_before * rate))
    end = min(audio.size, int(loud[-1]) + 1 + int(pad_after * rate))
    return audio[start:end]


def for_recognisers(audio, rate):
    """The clip as the recognisers hear it: 16 kHz, with half a second of silence either side.

    A recogniser tends to miss a first word that starts with no silence before
    it, which is how a trimmed clip starts; the padding is for them only.
    """
    from math import gcd

    import numpy as np
    from scipy.signal import resample_poly

    divisor = gcd(16000, rate)
    resampled = resample_poly(audio, 16000 // divisor, rate // divisor).astype(np.float32)
    silence = np.zeros(8000, dtype=np.float32)
    return np.concatenate([silence, resampled, silence])


HOP_SECONDS = 0.02


def runs(mask, minimum):
    """(first, last) frame of each run of True at least `minimum` frames long."""
    found, start = [], None
    for index, value in enumerate(list(mask) + [False]):
        if value and start is None:
            start = index
        elif not value and start is not None:
            if index - start >= minimum:
                found.append((start, index - 1))
            start = None
    return found


def inspect(audio, rate):
    """Finds the sounds in a clip that are not speech, which recognisers ignore.

    A generative voice that fails to stop cleanly can add a squeal or a burst
    after its last word, following a silence; that tail is cut, keeping a
    little of the silence. A burst louder than the speech, or a squeak far
    above the voice, before that point cannot be cut out, and is reported so
    the clip can be made again. Returns (audio, repaired, artefacts).
    """
    import librosa
    import numpy as np

    hop = int(HOP_SECONDS * rate)
    level = 20 * np.log10(np.maximum(librosa.feature.rms(y=audio, frame_length=2048, hop_length=hop)[0], 1e-9))
    f0, voiced, _ = librosa.pyin(audio, fmin=60, fmax=1200, sr=rate, frame_length=2048, hop_length=hop)
    frames = min(len(level), len(f0))
    level, f0, voiced = level[:frames], np.nan_to_num(f0[:frames]), voiced[:frames]
    at = lambda frame: round(frame * HOP_SECONDS, 2)
    speech = voiced & (f0 < 400)
    if speech.sum() < 5:
        return audio, None, [{"kind": "no speech found", "start": 0.0, "end": at(frames), "level": round(float(level.max()), 1)}]

    # A tail: sound after at least 0.3 s of silence that follows the last word,
    # cut only when it is not speech - much louder than the voice, or mostly
    # unvoiced. Voiced sound after a pause is words (a short last sentence the
    # pitch tracker missed), which the recognisers judge instead.
    audible = level > -45
    speech_level = float(np.percentile(level[speech], 95))
    cut, removed, quiet = None, None, 0
    for frame in range(int(np.flatnonzero(speech)[-1]) + 1, frames):
        if not audible[frame]:
            quiet += 1
            continue
        if quiet >= int(0.3 / HOP_SECONDS):
            tail = np.flatnonzero(audible[frame:]) + frame
            loud = float(level[tail].max()) > speech_level + 6
            unvoiced = float(np.mean(voiced[tail])) < 0.3
            if loud or unvoiced:
                cut = frame - quiet + int(0.15 / HOP_SECONDS)
                removed = {"cutAt": at(cut), "from": at(frame), "level": round(float(level[frame:].max()), 1), "why": "louder than the speech" if loud else "not voiced"}
            break
        quiet = 0
    if cut is not None:
        end = cut * hop
        fade = min(end, int(0.02 * rate))
        audio = audio[:end].copy()
        audio[end - fade:] *= np.linspace(1.0, 0.0, fade, dtype=audio.dtype)
        frames = min(frames, cut)

    median_f0 = float(np.median(f0[speech]))
    artefacts = []
    for first, last in runs(level[:frames] > speech_level + 6, 3):
        artefacts.append({"kind": "a burst louder than the speech", "start": at(first), "end": at(last + 1), "level": round(float(level[first:last + 1].max()), 1)})
    for first, last in runs(voiced[:frames] & (f0[:frames] > 2.5 * median_f0) & (level[:frames] > -45), 3):
        artefacts.append({"kind": "a squeak far above the voice", "start": at(first), "end": at(last + 1), "level": round(float(level[first:last + 1].max()), 1)})
    return audio, removed, artefacts


def main():
    args = parse()
    # The protocol gets its own handle on the real stdout; print() and the
    # libraries' output go to stderr from here on.
    protocol = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8", buffering=1)
    sys.stdout = sys.stderr
    sys.stdin.reconfigure(encoding="utf-8")

    def send(message):
        protocol.write(MARK + json.dumps(message) + "\n")
        protocol.flush()

    started = time.time()
    import importlib.metadata as metadata

    import numpy as np
    import soundfile as sf
    import torch
    from chatterbox.tts import ChatterboxTTS
    from faster_whisper import WhisperModel
    from huggingface_hub import hf_hub_download

    torch.set_num_threads(args.threads)
    paths = [hf_hub_download(repo_id=args.model, filename=name, revision=args.revision) for name in FILES]
    model = ChatterboxTTS.from_local(os.path.dirname(paths[0]), "cpu")
    model.prepare_conditionals(args.reference, exaggeration=args.exaggeration)
    recognisers = {
        name: WhisperModel(name, device="cpu", compute_type="int8", cpu_threads=args.threads)
        for name in filter(None, args.recognisers.split(","))
    }
    send({
        "ready": True,
        "sampleRate": model.sr,
        "loadSeconds": round(time.time() - started, 1),
        "versions": {
            "chatterbox-tts": metadata.version("chatterbox-tts"),
            "torch": torch.__version__,
            "faster-whisper": metadata.version("faster-whisper"),
            "model": f"{args.model}@{args.revision}",
        },
    })

    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        try:
            begun = time.time()
            if "analyse" in request:
                # A clip made before: look for artefacts, and write it without its tail if it has one.
                audio, _ = sf.read(request["analyse"], dtype="float32")
                audio, repaired, artefacts = inspect(audio, model.sr)
                if repaired:
                    sf.write(request["output"], audio, model.sr, subtype="PCM_16")
                send({
                    "id": request["id"],
                    "ok": True,
                    "seconds": round(audio.size / model.sr, 3),
                    "repaired": repaired,
                    "artefacts": artefacts,
                    "checkSeconds": round(time.time() - begun, 1),
                })
                continue
            torch.manual_seed(int(request["seed"]))
            wav = model.generate(
                request["text"],
                exaggeration=args.exaggeration,
                cfg_weight=args.cfg_weight,
                temperature=args.temperature,
            )
            raw = wav.squeeze(0).detach().cpu().numpy().astype(np.float32)
            audio = trim(raw, model.sr, args.trim_threshold_db, args.trim_pad_before, args.trim_pad_after)
            generated = time.time() - begun
            audio, repaired, artefacts = inspect(audio, model.sr)
            sf.write(request["output"], audio, model.sr, subtype="PCM_16")
            heard = for_recognisers(audio, model.sr)
            transcripts = {}
            for name, recogniser in recognisers.items():
                segments, _ = recogniser.transcribe(
                    heard,
                    language="en",
                    beam_size=5,
                    temperature=0.0,
                    condition_on_previous_text=False,
                    hotwords=args.hotwords or None,
                )
                transcripts[name] = " ".join(segment.text.strip() for segment in segments)
            send({
                "id": request["id"],
                "ok": True,
                "seconds": round(audio.size / model.sr, 3),
                "rawSeconds": round(raw.size / model.sr, 3),
                "transcripts": transcripts,
                "repaired": repaired,
                "artefacts": artefacts,
                "generateSeconds": round(generated, 1),
                "checkSeconds": round(time.time() - begun - generated, 1),
            })
        except Exception as error:  # reported to narrate.js, which decides what to do
            send({"id": request.get("id"), "ok": False, "error": f"{type(error).__name__}: {error}"})


if __name__ == "__main__":
    main()
