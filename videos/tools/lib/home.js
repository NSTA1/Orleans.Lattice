// The documentation site's home page (docs-site/pages/index.md), read for the
// words it puts on screen: the thesis, the three ways in, the deployment
// journey and its invariant, and the seams lede. The videos show the site's
// own words rather than a copy of them, so a change to the home page reaches
// every episode that quotes it. Each part fails loudly when the page no longer
// has the shape this reader expects, rather than putting stale or empty text
// on camera.

const decode = (text) =>
  text
    .replace(/&#(\d+);/g, (_, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCodePoint(parseInt(code, 16)))
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");

/** Text with its <code> spans kept as [text, isCode] parts and every other tag removed. */
export function textParts(html) {
  const parts = [];
  const pattern = /<code>([\s\S]*?)<\/code>/g;
  let last = 0;
  let match;
  const push = (text, code) => {
    const clean = decode(text.replace(/<[^>]+>/g, "")).replace(/\s+/g, " ");
    if (clean) parts.push([clean, code]);
  };
  while ((match = pattern.exec(html)) !== null) {
    push(html.slice(last, match.index), false);
    push(match[1], true);
    last = pattern.lastIndex;
  }
  push(html.slice(last), false);
  if (parts.length > 0) {
    parts[0][0] = parts[0][0].replace(/^\s+/, "");
    parts.at(-1)[0] = parts.at(-1)[0].replace(/\s+$/, "");
  }
  return parts.filter(([text]) => text.length > 0);
}

/** The plain text of an HTML fragment. */
export const plainText = (html) => textParts(html).map(([text]) => text).join("");

function required(pattern, html, what) {
  const match = pattern.exec(html);
  if (!match) {
    throw new Error(`home: docs-site/pages/index.md no longer has ${what}; update tools/lib/home.js to its new shape`);
  }
  return match;
}

/** The parts of the home page the videos put on screen. */
export function homePage(markdown) {
  const thesis = plainText(required(/<h1 id="lt-hero-title">([\s\S]*?)<\/h1>/, markdown, "the thesis heading")[1]);

  const ways = [...markdown.matchAll(/<a class="lt-way" href="#([a-z-]+)"><span class="lt-way-name">([\s\S]*?)<\/span><span class="lt-way-for">([\s\S]*?)<\/span><\/a>/g)].map(
    ([, anchor, name, forHtml]) => ({ anchor, name: plainText(name), for: textParts(forHtml) }),
  );
  if (ways.length !== 3) {
    throw new Error(`home: docs-site/pages/index.md has ${ways.length} way(s) in, not three; update tools/lib/home.js to its new shape`);
  }

  const stagesHtml = required(/<ol class="lt-stages">([\s\S]*?)<\/ol>/, markdown, "the deployment journey")[1];
  const stages = stagesHtml
    .split('<li class="lt-stage">')
    .slice(1)
    .map((block) => {
      const items = [...(required(/<ul>([\s\S]*?)<\/ul>/, block, "a stage's list")[1].matchAll(/<li>([\s\S]*?)<\/li>/g))].map(([, item]) => ({
        text: plainText(item.replace(/<span class="lt-status">[\s\S]*?<\/span>/g, "")),
        inProgress: /class="lt-status"/.test(item),
      }));
      return {
        name: plainText(required(/<h3>([\s\S]*?)<\/h3>/, block, "a stage name")[1]),
        caption: plainText(required(/<p>([\s\S]*?)<\/p>/, block, "a stage caption")[1]),
        items,
      };
    });
  if (stages.length === 0) {
    throw new Error("home: docs-site/pages/index.md lists no deployment stages; update tools/lib/home.js to its new shape");
  }
  const invariant = required(
    /<p class="lt-invariant"><span class="lt-invariant-label">([\s\S]*?)<\/span>\s*<code>([\s\S]*?)<\/code>\s*<span class="lt-invariant-note">([\s\S]*?)<\/span><\/p>/,
    markdown,
    "the journey's invariant",
  );

  const seams = required(
    /<h2 id="lt-seams-title">([\s\S]*?)<\/h2>\s*<p class="lt-section-lede">([\s\S]*?)<\/p>/,
    markdown,
    "the seams section",
  );

  return {
    thesis,
    ways,
    paths: { title: plainText(required(/<h2 id="lt-paths-title">([\s\S]*?)<\/h2>/, markdown, "the ways-in heading")[1]) },
    journey: {
      title: plainText(required(/<h2 id="lt-journey-title">([\s\S]*?)<\/h2>/, markdown, "the journey heading")[1]),
      stages,
      invariant: { label: plainText(invariant[1]), code: plainText(invariant[2]), note: plainText(invariant[3]) },
    },
    seams: { title: plainText(seams[1]), lede: plainText(seams[2]) },
  };
}

/**
 * The home page as a script compositions load before any component mounts.
 * Anything the site marks in progress stays off camera: the series shows only
 * what has shipped.
 */
export function homeScript(markdown, generatedBanner) {
  const home = homePage(markdown);
  for (const stage of home.journey.stages) {
    stage.items = stage.items.filter((item) => !item.inProgress).map((item) => item.text);
  }
  return `${generatedBanner}window.LatticeHome = Object.freeze(${JSON.stringify(home, null, 2)});\n`;
}
