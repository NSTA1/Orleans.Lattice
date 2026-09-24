import assert from "node:assert/strict";
import { test } from "node:test";
import {
  cutOf,
  episodeProblems,
  findVideoBlocks,
  formatLength,
  isCut,
  isMediaName,
  mediaNames,
  PATHS,
  posterTime,
  videoBlock,
} from "../lib/publication.js";

const valid = { path: "front-door", order: 1, poster: { scene: "opening", beat: 1, offset: -0.5 } };

test("an episode's metadata names its path, its place on it and its poster's moment", () => {
  assert.deepEqual(episodeProblems(valid), []);
  assert.deepEqual(episodeProblems({ ...valid, published: { cut: "0123456789ab", bytes: 10 } }), []);
  assert.deepEqual(episodeProblems({ path: "build", order: 3, poster: { scene: "close" } }), []);
});

test("metadata off the path vocabulary, out of order, or with a bad poster or cut is rejected, every problem at once", () => {
  assert.deepEqual(episodeProblems({ path: "sideways", order: 0, poster: { scene: "" }, extra: 1 }), [
    "unknown field 'extra'",
    `path must be one of ${PATHS.join(", ")}`,
    "order must be a whole number from 1",
    "poster.scene must name a scene of the composition",
  ]);
  assert.deepEqual(episodeProblems({ ...valid, poster: { scene: "a", beat: -1, offset: "soon", at: 2 } }), [
    "unknown field 'poster.at'",
    "poster.beat must be a whole number from 0",
    "poster.offset must be a number of seconds",
  ]);
  assert.deepEqual(episodeProblems({ ...valid, published: { cut: "XYZ", bytes: 0 } }), [
    "published.cut must be 12 lower-case hex digits",
    "published.bytes must be the size of the video in bytes",
  ]);
  assert.deepEqual(episodeProblems([]), ["expected a JSON object"]);
});

test("the paths are listed in the order the site shows them, front door first", () => {
  assert.equal(PATHS[0], "front-door");
  assert.deepEqual(PATHS.slice(1, 4), ["build", "evaluate", "operate"]);
});

test("a cut is 12 hex digits that change when any of the three files changes", () => {
  const digests = { mp4: "a".repeat(64), vtt: "b".repeat(64), jpg: "c".repeat(64) };
  const cut = cutOf(digests);
  assert.ok(isCut(cut), cut);
  assert.equal(cutOf({ ...digests }), cut, "the same files give the same cut");
  for (const ext of ["mp4", "vtt", "jpg"]) {
    assert.notEqual(cutOf({ ...digests, [ext]: "d".repeat(64) }), cut, `a new ${ext} gives a new cut`);
  }
  assert.equal(isCut("0123456789AB"), false, "upper case is not a cut");
});

test("a cut's files are named by the episode and the cut", () => {
  assert.deepEqual(mediaNames("introduction", "0123456789ab"), {
    mp4: "introduction-0123456789ab.mp4",
    vtt: "introduction-0123456789ab.vtt",
    jpg: "introduction-0123456789ab.jpg",
  });
});

test("a length is shown as minutes and seconds, rounded to the second", () => {
  assert.equal(formatLength(172.468), "2:52");
  assert.equal(formatLength(59.6), "1:00");
  assert.equal(formatLength(3.2), "0:03");
});

const composition = [
  '<div data-composition-id="episode" data-start="0" data-duration="60">',
  '  <div data-composition-id="title-card" data-scene="opening" data-start="0" data-duration="10"></div>',
  "  <div data-composition-id=\"places\" data-scene=\"many-places\" data-start=\"10\" data-duration=\"50\"",
  "       data-variable-values='{\"beats\":\"0.6,6.5,20.25\",\"end\":50}'></div>",
  '  <div class="clip" data-scene="many-places" data-start="30" data-duration="5"></div>',
  "</div>",
].join("\n");

test("a poster's moment is its scene's start, plus the named beat, plus the offset", () => {
  assert.equal(posterTime(composition, { scene: "many-places", beat: 2, offset: -0.5 }), 29.75);
  assert.equal(posterTime(composition, { scene: "many-places" }), 10);
  assert.equal(posterTime(composition, { scene: "opening", offset: 2 }), 2);
});

test("a poster outside the episode, on a missing scene or beat, or in an unstamped composition is rejected", () => {
  assert.throws(() => posterTime(composition, { scene: "absent" }), /scene 'absent' is not in the composition/);
  assert.throws(() => posterTime(composition, { scene: "many-places", beat: 3 }), /has 3 stamped beat\(s\), so it has no beat 3/);
  assert.throws(() => posterTime(composition, { scene: "many-places", offset: 60 }), /falls at 70s, outside the episode/);
  assert.throws(() => posterTime('<div data-composition-id="x"></div>', { scene: "opening" }), /not stamped with a duration/);
});

test("a published video block pins its cut and tells github.com readers where to watch or download it", () => {
  const block = videoBlock({
    slug: "introduction",
    path: "front-door",
    order: 1,
    length: "2:52",
    published: { cut: "0123456789ab", bytes: 10829648 },
  });
  assert.equal(
    block,
    [
      '<!-- video:begin episode="introduction" path="front-door" order="1" length="2:52" cut="0123456789ab" -->',
      "",
      "> [!NOTE]",
      "> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/introduction.html),",
      "> or [download it](../../docs-site/media/introduction-0123456789ab.mp4)",
      "> (MP4, 2:52, 10.8 MB).",
      "",
      "<!-- video:end -->",
    ].join("\n"),
  );
});

test("only a slug, a dash, a cut and a media extension name a published file", () => {
  assert.equal(isMediaName("introduction-0123456789ab.mp4"), true);
  assert.equal(isMediaName("hello-lattice-0123456789ab.vtt"), true);
  assert.equal(isMediaName("hello-lattice-0123456789ab.jpg"), true);
  assert.equal(isMediaName("introduction-0123456789ab.png"), false);
  assert.equal(isMediaName("introduction-0123456789a.mp4"), false);
  assert.equal(isMediaName("README.md"), false);
});

test("an unpublished video block has no cut, and reads its line breaks from the page", () => {
  const block = videoBlock({ slug: "hello", path: "build", order: 2, length: "4:10" }, "\r\n");
  assert.equal(
    block,
    '<!-- video:begin episode="hello" path="build" order="2" length="4:10" -->\r\n\r\n> [!NOTE]\r\n> The video is not published yet.\r\n\r\n<!-- video:end -->',
  );
});

test("a video block is found again with its attributes and its extent", () => {
  const block = videoBlock({ slug: "hello", path: "build", order: 2, length: "4:10", published: { cut: "abcdefabcdef", bytes: 5 } });
  const page = `# Hello\n\nIntro.\n\n${block}\n\n## Transcript\n`;
  const [found, ...rest] = findVideoBlocks(page);
  assert.equal(rest.length, 0);
  assert.deepEqual(found.attributes, { episode: "hello", path: "build", order: "2", length: "4:10", cut: "abcdefabcdef" });
  assert.equal(page.slice(found.start, found.end), block);
});

test("a video block without an end is found with no extent, and a begin marker in another form is an error", () => {
  assert.equal(findVideoBlocks('<!-- video:begin episode="a" -->\ntext')[0].end, -1);
  assert.throws(() => findVideoBlocks("<!-- video:begin episode=a -->\n<!-- video:end -->"), /not in the form/);
  assert.deepEqual(findVideoBlocks("# A page with no video"), []);
});
