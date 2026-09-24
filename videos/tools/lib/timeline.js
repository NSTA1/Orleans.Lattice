// Stamps an episode's narration timeline (renders/narration/<slug>/cues.json,
// written by tools/narrate.js) into its composition. HyperFrames reads timing
// statically from HTML attributes, so the timeline cannot be applied by a
// script at render time; it is written into the file instead, and rewritten
// whenever the narration changes.
//
// In the composition:
//
//   data-scene="<id>"       a clip that shows a scene (the id is the scene's
//                           "### " heading in SCRIPT.md, in lower-case words
//                           joined by hyphens). It gets the scene's window as
//                           data-start and data-duration; a component host also
//                           gets its beats and its end as variables.
//   data-from-beat="<n>"    with data-scene: the clip starts on the scene's
//                           nth beat (0-based) instead of with the scene.
//   <!-- narration:begin --> ... <!-- narration:end -->
//                           regenerated as the <audio> element that plays the
//                           episode's mastered narration.
//
// The root composition gets the episode's duration.

const BASIC_ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" };

/** Decodes the character references an attribute value may hold; any other named entity is an error. */
export function decodeEntities(text) {
  return text.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (whole, ref) => {
    if (ref[0] === "#") {
      const code = ref[1] === "x" || ref[1] === "X" ? parseInt(ref.slice(2), 16) : parseInt(ref.slice(1), 10);
      return String.fromCodePoint(code);
    }
    const named = BASIC_ENTITIES[ref.toLowerCase()];
    if (named === undefined) {
      throw new Error(`timeline: write ${whole} as a numeric reference (&#...;), so the file stays plain ASCII and unambiguous`);
    }
    return named;
  });
}

/** Encodes an attribute value for the given quote: &, the quote and every non-ASCII character become references. */
export function encodeAttribute(text, quote = '"') {
  let out = "";
  for (const char of text) {
    const code = char.codePointAt(0);
    if (char === "&") out += "&amp;";
    else if (char === quote) out += quote === '"' ? "&quot;" : "&#39;";
    else if (code > 0x7e || code < 0x20) out += `&#${code};`;
    else out += char;
  }
  return out;
}

const RAW_TEXT = new Set(["script", "style", "textarea", "title"]);

/**
 * Every opening tag in a document, in order, with its position and its
 * attributes (each with its own position and decoded value). Comments, the
 * doctype, closing tags and the text of script and style elements are
 * skipped.
 */
export function openingTags(html) {
  const tags = [];
  const lower = html.toLowerCase();
  let i = 0;
  while (i < html.length) {
    const lt = html.indexOf("<", i);
    if (lt < 0) break;
    if (html.startsWith("<!--", lt)) {
      const close = html.indexOf("-->", lt + 4);
      i = close < 0 ? html.length : close + 3;
      continue;
    }
    const name = /^<([a-zA-Z][a-zA-Z0-9-]*)/.exec(html.slice(lt, lt + 64));
    if (!name) {
      const close = html.indexOf(">", lt);
      i = close < 0 ? html.length : close + 1;
      continue;
    }
    const attrs = [];
    let j = lt + name[0].length;
    let selfClosing = false;
    for (;;) {
      while (j < html.length && /\s/.test(html[j])) j++;
      if (j >= html.length) throw new Error(`timeline: the <${name[1]}> tag at offset ${lt} is never closed`);
      if (html[j] === ">") {
        j++;
        break;
      }
      if (html.startsWith("/>", j)) {
        j += 2;
        selfClosing = true;
        break;
      }
      const start = j;
      while (j < html.length && !/[\s=>]/.test(html[j]) && !html.startsWith("/>", j)) j++;
      const attrName = html.slice(start, j).toLowerCase();
      let k = j;
      while (k < html.length && /\s/.test(html[k])) k++;
      let value = "";
      let quote = null;
      if (html[k] === "=") {
        k++;
        while (k < html.length && /\s/.test(html[k])) k++;
        if (html[k] === '"' || html[k] === "'") {
          quote = html[k];
          const close = html.indexOf(quote, k + 1);
          if (close < 0) throw new Error(`timeline: the ${attrName} attribute at offset ${start} is never closed`);
          value = html.slice(k + 1, close);
          j = close + 1;
        } else {
          const valueStart = k;
          while (k < html.length && !/[\s>]/.test(html[k])) k++;
          value = html.slice(valueStart, k);
          j = k;
        }
      }
      attrs.push({ name: attrName, raw: value, quote, start, end: j });
    }
    const tag = { name: name[1].toLowerCase(), start: lt, end: j, selfClosing, attrs };
    tag.get = (attr) => {
      const found = attrs.find((candidate) => candidate.name === attr);
      return found === undefined ? undefined : decodeEntities(found.raw);
    };
    tags.push(tag);
    if (RAW_TEXT.has(tag.name) && !selfClosing) {
      const close = lower.indexOf(`</${tag.name}`, j);
      i = close < 0 ? html.length : close;
    } else {
      i = j;
    }
  }
  return tags;
}

/**
 * The edits that give a tag new attribute values: an attribute it has is
 * replaced where it stands; a new one follows its last attribute, on a line of
 * its own when the tag's attributes are one per line.
 */
function attributeEdits(html, tag, updates) {
  const edits = [];
  const additions = [];
  for (const [name, { value, quote }] of Object.entries(updates)) {
    const text = `${name}=${quote}${encodeAttribute(value, quote)}${quote}`;
    const existing = tag.attrs.find((attr) => attr.name === name);
    if (existing) {
      edits.push({ start: existing.start, end: existing.end, text });
    } else {
      additions.push(text);
    }
  }
  if (additions.length > 0) {
    const last = tag.attrs.at(-1);
    const anchor = last ? last.end : tag.start + 1 + tag.name.length;
    const lineStart = html.lastIndexOf("\n", last ? last.start : tag.start) + 1;
    const multiline = last && html.slice(lineStart, last.start).trim() === "" && lineStart > tag.start;
    const separator = multiline ? `\n${html.slice(lineStart, last.start)}` : " ";
    edits.push({ start: anchor, end: anchor, text: additions.map((text) => separator + text).join("") });
  }
  return edits;
}

/** Applies non-overlapping edits ({ start, end, text }) to a string. */
function applyEdits(text, edits) {
  let out = text;
  for (const edit of [...edits].sort((a, b) => b.start - a.start)) {
    out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
  }
  return out;
}

const seconds = (value) => String(Math.round(value * 1000) / 1000);

/**
 * The composition with the narration's timeline stamped in. `clipRoot` is the
 * root-relative folder the clip paths in the manifest are relative to.
 * Throws when the composition and the narration disagree: a scene the
 * composition does not show, a clip for a scene the script does not have, a
 * beat the scene does not reach, or missing narration markers.
 */
export function stampComposition(html, manifest, { clipRoot, trackIndex = 1 }) {
  const tags = openingTags(html);
  const edits = [];
  const root = tags.find((tag) => tag.get("data-composition-id") !== undefined);
  if (!root) throw new Error("timeline: the composition has no root element with data-composition-id");
  edits.push(...attributeEdits(html, root, { "data-duration": { value: seconds(manifest.duration), quote: '"' } }));

  const scenes = new Map(manifest.scenes.map((scene) => [scene.id, scene]));
  const shown = new Set();
  for (const tag of tags) {
    const id = tag.get("data-scene");
    if (id === undefined) continue;
    const scene = scenes.get(id);
    if (!scene) {
      throw new Error(`timeline: a <${tag.name}> shows scene '${id}', which SCRIPT.md does not have (it has ${[...scenes.keys()].join(", ")})`);
    }
    shown.add(id);
    const fromBeat = tag.get("data-from-beat");
    const first = fromBeat === undefined ? null : Number(fromBeat);
    if (first !== null && !(Number.isInteger(first) && first >= 0 && first < scene.beats.length)) {
      throw new Error(`timeline: scene '${id}' has ${scene.beats.length} beat(s), so data-from-beat="${fromBeat}" is out of range`);
    }
    const offset = first === null ? 0 : scene.beats[first];
    const start = scene.start + offset;
    const duration = scene.end - start;
    const updates = {
      "data-start": { value: seconds(start), quote: '"' },
      "data-duration": { value: seconds(duration), quote: '"' },
    };
    if (tag.get("data-composition-src") !== undefined) {
      const raw = tag.get("data-variable-values");
      const variables = raw === undefined || raw.trim() === "" ? {} : JSON.parse(raw);
      variables.beats = scene.beats
        .slice(first ?? 0)
        .map((beat) => seconds(beat - offset))
        .join(",");
      variables.end = Number(seconds(duration));
      updates["data-variable-values"] = { value: JSON.stringify(variables), quote: "'" };
    }
    edits.push(...attributeEdits(html, tag, updates));
  }
  const unshown = [...scenes.keys()].filter((id) => !shown.has(id));
  if (unshown.length > 0) {
    throw new Error(`timeline: the composition shows no clip for scene(s) ${unshown.join(", ")}; add an element with data-scene="<id>" for each`);
  }

  const begin = html.indexOf("<!-- narration:begin -->");
  const end = html.indexOf("<!-- narration:end -->");
  if (begin < 0 || end < begin) {
    throw new Error("timeline: the composition needs <!-- narration:begin --> and <!-- narration:end --> inside its root");
  }
  if (!manifest.narration) {
    throw new Error("timeline: the narration was never joined and mastered (narrate needs ffmpeg for that); install ffmpeg and narrate again");
  }
  const lineStart = html.lastIndexOf("\n", begin) + 1;
  const indent = html.slice(lineStart, begin);
  // One mastered track for the whole episode: the cues are already placed on
  // the timeline inside it, and it is the file the loudness was measured on.
  const audio =
    `${indent}<audio id="narration" src="${clipRoot}/${manifest.narration}" data-start="0" ` +
    `data-duration="${seconds(manifest.duration)}" data-track-index="${trackIndex}"></audio>`;
  const bodyStart = begin + "<!-- narration:begin -->".length;
  edits.push({ start: bodyStart, end, text: `\n${audio}\n${indent}` });
  return applyEdits(html, edits);
}
