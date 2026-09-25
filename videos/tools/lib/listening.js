// The listening pages. The narration page (npm run review -- <slug> --audio)
// is an episode's narration alone, every cue listed so a click plays from it,
// with what each cue's check found. The takes page (npm run audition) lays out
// several takes of a line to pick from. Approving the voice by ear comes before
// rendering the video, because re-speaking a cue takes minutes and a render the
// better part of ten.
import { escapeHtml } from "./snippets.js";

/** A time as m:ss.s. */
export function clock(seconds) {
  return `${Math.floor(seconds / 60)}:${(seconds % 60).toFixed(1).padStart(4, "0")}`;
}

/**
 * What a listener should know about a cue, from its entry in cues.json: what
 * the last narration run changed (`changed`), which take was kept, and what
 * the check could not settle, which only an ear can.
 */
export function cueNotes(cue) {
  const notes = [];
  if (cue.changed) notes.push(`changed in this run: ${cue.changed}`);
  const check = cue.check;
  if (check?.picked) notes.push(check.attempt ? `take ${check.attempt}, picked by ear` : "picked by ear");
  else if (check?.attempt > 1) notes.push(`take ${check.attempt}`);
  if (check && !check.verified) {
    const heard = Object.entries(check.differences ?? {})
      .filter(([, differences]) => differences.length > 0)
      .map(([recogniser, differences]) => `${recogniser} heard ${differences.join(", ")}`);
    notes.push(`listen: ${[...heard, ...(check.problems ?? [])].join("; ") || "it did not pass its check"}`);
  }
  return notes;
}

/**
 * The page: `title`, the episode's `manifest` (cues.json), the narration's
 * `audioSrc` relative to the page, and the stylesheets to link. A cue is
 * highlighted when it has a note that asks for a listen or a change to hear.
 */
export function listeningPage({ title, manifest, audioSrc, stylesheets = [] }) {
  const rows = manifest.cues.map((cue) => {
    const notes = cueNotes(cue);
    const flagged = Boolean(cue.changed) || (cue.check && !cue.check.verified);
    return [
      `<li${flagged ? ' class="flag"' : ""}>`,
      `<button type="button" data-t="${cue.start}">${clock(cue.start)}</button>`,
      `<span class="text">${escapeHtml(cue.text)}`,
      ...notes.map((note) => `<span class="note">${escapeHtml(note)}</span>`),
      "</span></li>",
    ].join("");
  });
  const level = manifest.loudness?.mastered ? `, mastered to ${manifest.loudness.mastered.integrated} LUFS` : "";
  const flaggedCount = manifest.cues.filter((cue) => cue.changed || (cue.check && !cue.check.verified)).length;
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)} - the narration</title>
${stylesheets.map((href) => `    <link rel="stylesheet" href="${href}" />`).join("\n")}
    <style>
      body { margin: 0; background: var(--lt-surface); color: var(--lt-ink); font-family: var(--lv-font-sans); line-height: 1.55; }
      main { max-width: 960px; margin: 0 auto; padding: 32px 24px 64px; }
      h1 { margin: 0 0 4px; font-size: var(--lt-text-2xl); font-weight: var(--lt-weight-display); letter-spacing: -0.02em; }
      .kicker { margin: 0 0 12px; color: var(--lt-ink-3); font-weight: var(--lt-weight-label); }
      audio { position: sticky; top: 0; width: 100%; padding: 8px 0; background: var(--lt-surface); }
      ol { list-style: none; padding: 0; }
      li { display: flex; gap: 12px; padding: 6px 8px; border-radius: var(--lt-radius-md); }
      li.flag { background: var(--lt-marker-soft); }
      li.playing { outline: 1.5px solid var(--lt-ink); }
      button { min-width: 5.5em; font: inherit; font-family: var(--lv-font-mono); background: transparent; color: var(--lt-ink); border: 1px solid var(--lt-rule); border-radius: var(--lt-radius-md); cursor: pointer; }
      .text { max-width: 72ch; }
      .note { display: block; color: var(--lt-ink-3); font-size: 0.9em; }
    </style>
  </head>
  <body>
    <main>
      <p class="kicker">Orleans.Lattice video series - listen before rendering</p>
      <h1>${escapeHtml(title)}</h1>
      <p>The narration alone, ${clock(manifest.duration)}${level}, in the series voice. Click a time to play from that cue. ${flaggedCount} cue(s) are highlighted: changed in the last narration run, or not settled by the check.</p>
      <audio id="narration" controls preload="auto" src="${audioSrc}"></audio>
      <ol>
        ${rows.join("\n        ")}
      </ol>
    </main>
    <script>
      const audio = document.getElementById("narration");
      const items = [...document.querySelectorAll("li")];
      const starts = items.map((item) => Number(item.querySelector("button").dataset.t));
      items.forEach((item, index) => item.querySelector("button").addEventListener("click", () => { audio.currentTime = starts[index]; audio.play(); }));
      audio.addEventListener("timeupdate", () => {
        let current = -1;
        starts.forEach((start, index) => { if (audio.currentTime >= start) current = index; });
        items.forEach((item, index) => item.classList.toggle("playing", index === current));
      });
    </script>
  </body>
</html>
`;
}

/**
 * The takes page (npm run audition): each auditioned cue with its takes, a
 * player for each and what its checks found, the take the narration uses
 * marked, and the command that picks one. `cues` is
 * [{ index, start, text, takes: [{ take, src, seconds, verdict, current }] }].
 */
export function takesPage({ title, slug, cues, stylesheets = [] }) {
  const sections = cues.map((cue) => {
    const where = typeof cue.start === "number" ? ` at ${clock(cue.start)}` : "";
    const takes = cue.takes.map((take) =>
      [
        `<li${take.current ? ' class="current"' : ""}>`,
        `<h3>Take ${take.take}${take.current ? " <span class=\"badge\">in the narration now</span>" : ""}</h3>`,
        `<audio controls preload="metadata" src="${take.src}"></audio>`,
        `<p class="note">${escapeHtml(`${typeof take.seconds === "number" ? `${take.seconds.toFixed(1)}s - ` : ""}${take.verdict}`)}</p>`,
        `<p class="pick"><code>npm run audition -- ${escapeHtml(slug)} --pick ${cue.index}=${take.take}</code></p>`,
        "</li>",
      ].join(""),
    );
    return `<section><h2>Cue ${cue.index}${where}</h2><p class="text">${escapeHtml(cue.text)}</p><ol>${takes.join("")}</ol></section>`;
  });
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)} - takes</title>
${stylesheets.map((href) => `    <link rel="stylesheet" href="${href}" />`).join("\n")}
    <style>
      body { margin: 0; background: var(--lt-surface); color: var(--lt-ink); font-family: var(--lv-font-sans); line-height: 1.55; }
      main { max-width: 960px; margin: 0 auto; padding: 32px 24px 64px; }
      h1 { margin: 0 0 4px; font-size: var(--lt-text-2xl); font-weight: var(--lt-weight-display); letter-spacing: -0.02em; }
      h2 { margin: 32px 0 4px; padding-top: 16px; border-top: 1px solid var(--lt-rule); font-size: var(--lt-text-xl); }
      h3 { margin: 0; font-size: var(--lt-text-lg); }
      .kicker { margin: 0 0 12px; color: var(--lt-ink-3); font-weight: var(--lt-weight-label); }
      .text { max-width: 72ch; }
      ol { list-style: none; padding: 0; }
      li { margin: 12px 0; padding: 8px; border-radius: var(--lt-radius-md); }
      li.current { background: var(--lt-marker-soft); }
      .badge { font-size: 0.8em; font-weight: var(--lt-weight-label); color: var(--lt-ink-2); }
      audio { width: 100%; }
      .note { margin: 4px 0; color: var(--lt-ink-3); font-size: 0.9em; }
      .pick { margin: 0; font-size: 0.85em; }
      code { font-family: var(--lv-font-mono); }
    </style>
  </head>
  <body>
    <main>
      <p class="kicker">Orleans.Lattice video series - takes to pick from</p>
      <h1>${escapeHtml(title)}</h1>
      <p>Each take of a line, made with the series voice and checked like any narration clip. Listen, pick one with the command under it, then run <code>npm run narrate -- ${escapeHtml(slug)}</code>. Takes and picks stay on this machine; only the published cut is committed.</p>
      ${sections.join("\n      ")}
    </main>
  </body>
</html>
`;
}