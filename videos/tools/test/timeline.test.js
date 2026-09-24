import assert from "node:assert/strict";
import { test } from "node:test";
import { decodeEntities, encodeAttribute, openingTags, stampComposition } from "../lib/timeline.js";

const manifest = {
  duration: 20.5,
  narration: "narration.wav",
  loudness: { mastered: { integrated: -16, truePeak: -1.8 } },
  scenes: [
    { id: "opening", title: "Opening", start: 0, end: 6.2, beats: [1.5], cues: [1] },
    { id: "the-join", title: "The join", start: 6.2, end: 20.5, beats: [0.6, 4.1, 9.35], cues: [2, 3, 4] },
  ],
  cues: [
    { index: 1, file: "clips/aaa.wav", scene: "opening", start: 1.5, end: 5.25 },
    { index: 2, file: "clips/bbb.wav", scene: "the-join", start: 6.8, end: 9.9 },
    { index: 3, file: "clips/ccc.wav", scene: "the-join", start: 10.3, end: 15 },
    { index: 4, file: "clips/ddd.wav", scene: "the-join", start: 15.55, end: 17.5 },
  ],
};

const composition = `<!doctype html>
<html>
  <head>
    <script>
      const trap = '<div data-scene="nope">';
    </script>
  </head>
  <body>
    <div id="root" data-composition-id="episode" data-start="0" data-width="1920">
      <div
        id="title"
        data-composition-id="title-card"
        data-composition-src="shared/components/title-card.html"
        data-scene="opening"
        data-variable-values='{"title":"It&#39;s here &#8852;"}'
      ></div>
      <div id="join" data-composition-id="join-diagram" data-composition-src="shared/components/join-diagram.html" data-scene="the-join" data-start="99"></div>
      <div id="rule-note" class="clip" data-scene="the-join" data-from-beat="2"></div>
      <!-- narration:begin -->
      <audio id="stale" src="x.wav"></audio>
      <!-- narration:end -->
    </div>
  </body>
</html>
`;

test("references decode, and encode back to plain ASCII", () => {
  assert.equal(decodeEntities("a &amp; b &lt;c&gt; &quot;d&quot; &#39;e&#39; &#x2294; &#8852;"), "a & b <c> \"d\" 'e' \u2294 \u2294");
  assert.throws(() => decodeEntities("&rarr;"), /numeric reference/);
  assert.equal(encodeAttribute("It's \u2294 & more", "'"), "It&#39;s &#8852; &amp; more");
  assert.equal(encodeAttribute('say "hi"', '"'), "say &quot;hi&quot;");
});

test("opening tags are read with their attributes, skipping comments and script text", () => {
  const tags = openingTags(composition);
  assert.ok(!tags.some((tag) => tag.get("data-scene") === "nope"), "a tag inside a script is not a tag");
  const title = tags.find((tag) => tag.get("id") === "title");
  assert.equal(title.get("data-variable-values"), '{"title":"It\'s here \u2294"}');
  assert.equal(tags.find((tag) => tag.get("id") === "stale").name, "audio");
  assert.equal(openingTags("<p hidden>x</p>")[0].get("hidden"), "");
});

test("each scene clip gets its scene's window; a component host also gets its beats and its end", () => {
  const stamped = stampComposition(composition, manifest, { clipRoot: "renders/narration/episode" });
  const tags = openingTags(stamped);
  const byId = (id) => tags.find((tag) => tag.get("id") === id);

  assert.equal(byId("root").get("data-duration"), "20.5");
  assert.equal(byId("title").get("data-start"), "0");
  assert.equal(byId("title").get("data-duration"), "6.2");
  assert.deepEqual(JSON.parse(byId("title").get("data-variable-values")), { title: "It's here \u2294", beats: "1.5", end: 6.2 });

  assert.equal(byId("join").get("data-start"), "6.2", "an existing timing attribute is replaced where it stands");
  assert.equal(byId("join").get("data-duration"), "14.3");
  assert.deepEqual(JSON.parse(byId("join").get("data-variable-values")), { beats: "0.6,4.1,9.35", end: 14.3 });

  assert.equal(byId("rule-note").get("data-start"), "15.55", "data-from-beat starts the clip on that beat");
  assert.equal(byId("rule-note").get("data-duration"), "4.95");
  assert.equal(byId("rule-note").get("data-variable-values"), undefined, "only a component host takes variables");
});

test("new attributes follow the tag's layout, and the file stays plain ASCII", () => {
  const stamped = stampComposition(composition, manifest, { clipRoot: "renders/narration/episode" });
  assert.match(stamped, /\n        data-variable-values='\{"title":"It&#39;s here &#8852;","beats":"1.5","end":6.2\}'\n        data-start="0"\n        data-duration="6.2"\n      ><\/div>/);
  assert.match(stamped, /data-scene="the-join" data-start="6.2" data-duration="14.3" data-variable-values=/);
  assert.ok(/^[\x09\x0a\x0d\x20-\x7e]*$/.test(stamped), "no character outside plain ASCII");
});

test("the narration block holds the one mastered track, for the whole episode", () => {
  const stamped = stampComposition(composition, manifest, { clipRoot: "renders/narration/episode" });
  const block = stamped.slice(stamped.indexOf("<!-- narration:begin -->"), stamped.indexOf("<!-- narration:end -->"));
  assert.ok(!block.includes("stale"));
  const audio = openingTags(block).filter((tag) => tag.name === "audio");
  assert.deepEqual(
    audio.map((tag) => [tag.get("id"), tag.get("src"), tag.get("data-start"), tag.get("data-duration"), tag.get("data-volume")]),
    [["narration", "renders/narration/episode/narration.wav", "0", "20.5", undefined]],
  );
  assert.match(stamped, /\n      <!-- narration:end -->/, "the end marker keeps its indentation");
  assert.throws(
    () => stampComposition(composition, { ...manifest, narration: null }, { clipRoot: "r" }),
    /never joined and mastered/,
  );
});

test("stamping is idempotent, so a check can compare the file with a fresh stamp", () => {
  const once = stampComposition(composition, manifest, { clipRoot: "renders/narration/episode" });
  assert.equal(stampComposition(once, manifest, { clipRoot: "renders/narration/episode" }), once);
});

test("a composition that disagrees with the narration is refused", () => {
  const stamp = (html, m = manifest) => stampComposition(html, m, { clipRoot: "r" });
  assert.throws(() => stamp(composition.replace('data-scene="opening"', 'data-scene="prologue"')), /scene 'prologue', which SCRIPT.md does not have/);
  assert.throws(
    () => stamp(composition, { ...manifest, scenes: [...manifest.scenes, { id: "close", start: 20, end: 21, beats: [0.6], cues: [] }] }),
    /no clip for scene\(s\) close/,
  );
  assert.throws(() => stamp(composition.replace('data-from-beat="2"', 'data-from-beat="3"')), /has 3 beat\(s\), so data-from-beat="3" is out of range/);
  assert.throws(() => stamp(composition.replace("<!-- narration:begin -->", "")), /narration:begin/);
  assert.throws(() => stamp(composition.replace('data-composition-id="episode" ', "").replace(/data-composition-id="[^"]+"/g, "")), /no root element/);
});
