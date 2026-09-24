// Orleans.Lattice video series - the scene runtime.
//
// What every scene component does the same way, so no component does it
// differently. Load it after motion.js and the site data (join-figures.js,
// packages.js, home.js).
//
//   LatticeScene.cue(vars)            a scene's beats and end, as stamped into
//                                     its variables by `npm run timeline`
//   LatticeScene.exit(tl, root, cue)  fade a scene's content out just before
//                                     its clip ends, so scenes never hard-cut
//   LatticeScene.site(value)          a variable written "site:<path>" is read
//                                     from the site's own words (home.js,
//                                     packages.js), anything else is returned
//                                     as written
//   LatticeScene.parts(element, parts)
//                                     set [text, isCode] parts as text, with
//                                     code spans in the mono face
(function () {
  const { duration } = window.LatticeMotion;

  function cue(vars) {
    const beats = String(vars.beats == null ? "" : vars.beats)
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map(Number);
    if (beats.some((beat) => !Number.isFinite(beat))) {
      throw new Error("scene: beats must be numbers, got '" + vars.beats + "'");
    }
    const end = Number(vars.end) > 0 ? Number(vars.end) : null;
    return {
      beats: beats,
      // The nth beat, or `fallback` when the scene is not timed to a script
      // (in the smoke test, or when previewing a component on its own).
      at: function (n, fallback) {
        return Number.isFinite(beats[n]) ? beats[n] : fallback;
      },
      end: end,
    };
  }

  function exit(tl, root, timing) {
    if (!timing.end) return;
    const targets = Array.from(root.children).filter((element) => element.tagName !== "STYLE" && element.tagName !== "SCRIPT");
    const length = duration.fast;
    tl.to(targets, { opacity: 0, duration: length, ease: "power1.in" }, Math.max(0, timing.end - length));
  }

  function site(value) {
    if (typeof value !== "string" || !value.startsWith("site:")) return value;
    const sources = { home: window.LatticeHome, packages: window.LatticePackages };
    const path = value.slice("site:".length).split(".");
    let found = sources;
    for (const key of path) {
      found = found == null ? undefined : found[key];
    }
    if (found === undefined) {
      throw new Error("scene: the site has no '" + value.slice(5) + "'; is shared/brand/site synced, and is its script loaded?");
    }
    return found;
  }

  function parts(element, value) {
    element.textContent = "";
    for (const [text, code] of value) {
      if (code) {
        const span = document.createElement("span");
        span.className = "lv-mono";
        span.textContent = text;
        element.appendChild(span);
      } else {
        element.appendChild(document.createTextNode(text));
      }
    }
  }

  window.LatticeScene = Object.freeze({ cue: cue, exit: exit, site: site, parts: parts });
})();
