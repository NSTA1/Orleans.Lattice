// Orleans.Lattice video series - motion.
//
// Registers the site's motion with GSAP so compositions move the way the
// documentation site does. Load it after gsap, CustomEase and MotionPathPlugin
// (all from node_modules) and after shared/brand/brand.css.
//
//   "lattice-out"   the site's --lt-ease-out, read from its tokens: entrances,
//                   state changes, the join's pulse
//   "lattice-draw"  the curve of the site's join figure: edges drawing and
//                   deltas travelling (docs-site main.css and main.js)
//
// LatticeMotion.duration holds the site's --lt-duration-* tokens in seconds;
// LatticeMotion.figure holds the join figure's choreography, which lives in the
// site's stylesheet and script rather than in its tokens.
(function () {
  const tokens = getComputedStyle(document.documentElement);
  const read = (name) => {
    const value = tokens.getPropertyValue(name).trim();
    if (!value) {
      throw new Error("motion: the site token " + name + " is not defined; is shared/brand/site/tokens.css synced?");
    }
    return value;
  };
  const bezier = (name) => {
    const match = /^cubic-bezier\(([^)]+)\)$/.exec(read(name));
    if (!match) throw new Error("motion: " + name + " is not a cubic-bezier()");
    return match[1].split(",").map((part) => part.trim()).join(",");
  };
  const seconds = (name) => {
    const match = /^([\d.]+)(ms|s)$/.exec(read(name));
    if (!match) throw new Error("motion: " + name + " is not a duration");
    return Number(match[1]) / (match[2] === "ms" ? 1000 : 1);
  };

  gsap.registerPlugin(CustomEase, MotionPathPlugin);
  CustomEase.create("lattice-out", bezier("--lt-ease-out"));
  CustomEase.create("lattice-draw", "0.45,0,0.2,1");

  window.LatticeMotion = Object.freeze({
    duration: Object.freeze({
      fast: seconds("--lt-duration-fast"),
      base: seconds("--lt-duration"),
      slow: seconds("--lt-duration-slow"),
    }),
    figure: Object.freeze({
      draw: 1.3,
      travel: 2.6,
      pulse: 0.9,
      redeliver: 1.4,
    }),
  });
})();
