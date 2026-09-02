/*
  Orleans.Lattice Explorer - appearance bootstrap.

  This file exists to solve exactly one problem: the operator's palette has to be
  on the document at the FIRST paint, not after the application has started. A
  light-theme operator who sees the dark palette for a moment on every single load
  has, in practice, no light theme at all.

  Nothing else in the Explorer can do that job:

    - The server cannot. The preference lives in the browser's storage, and the
      head prerenders before any circuit exists, so the markup it produces cannot
      know the answer.
    - A component cannot. The attributes belong on <html> and <body>, above every
      component's render tree, and a component only runs after hydration - which
      is precisely the moment that is too late.

  A classic <script> in <head> with no `defer` and no `async` does: the parser
  blocks on it, and the parser has not reached <body>, so nothing has been laid
  out and nothing has been painted when it runs. That is the whole trick, and it
  is why this file must stay a classic script and must stay in <head>.

  WHAT IT READS. A plain, unencrypted first-paint record in localStorage. The
  durable preference contract (IExplorerShellPreferences) remains the only thing
  that REMEMBERS a preference; its web backing store encrypts the document with
  Data Protection, so script cannot read it, and it is not reachable until a
  circuit exists in any case. This record is therefore a CACHE of the last applied
  appearance, refreshed on every apply. When the two disagree the contract wins,
  because the application rewrites this record from it as soon as it loads. The
  values are non-secret presentation names and are validated against a fixed
  allow-list on the way in, so a tampered record can only ever select one of the
  appearances the product already ships.

  WHERE EACH ATTRIBUTE GOES.

    data-theme, data-contrast  ->  <html>, because the token layer selects on
                                   :root for both.
    data-lx-density            ->  <body>, deliberately NOT <html>. The token
                                   layer declares each density preset at
                                   attribute specificity, and the breakpoint layer
                                   declares the compact band's own density on
                                   :root at the same specificity but from a later
                                   stylesheet - so on <html> the band would win
                                   and an explicit choice would be silently
                                   ignored on a narrow viewport. Nothing declares
                                   density on <body>, so the choice wins there at
                                   every width, and lattice-appearance.css makes
                                   every adaptive root below defer to it.

  Because <body> does not exist while <head> is parsing, the density stamp is
  applied by `latticeAppearance.stamp()`, which each head calls once at the top of
  <body>, before any shell content is parsed. A DOMContentLoaded fallback covers a
  head that forgets.
*/
(function () {
    "use strict";

    var RECORD_KEY = "orleans.lattice.explorer.appearance.v1";

    /* The complete set of values this build understands. Anything else in the
       record is discarded rather than written onto the document. */
    var THEMES = { light: 1, dark: 1 };
    var CONTRASTS = { standard: 1, more: 1 };
    var DENSITIES = { comfortable: 1, cosy: 1, compact: 1 };

    /* null on an axis means "follow the environment", which is the absence of a
       choice rather than a value, so it is expressed as an absent attribute. */
    var current = { theme: null, contrast: null, density: null };

    function permitted(table, value) {
        if (typeof value !== "string") {
            return null;
        }
        return Object.prototype.hasOwnProperty.call(table, value) ? value : null;
    }

    function readRecord() {
        var raw = null;
        try {
            raw = window.localStorage.getItem(RECORD_KEY);
        } catch (e) {
            /* Storage can be unavailable (privacy mode, a blocked origin). The
               environment's own preference is then the only input, which is the
               correct answer for a browser that will not remember anything. */
            raw = null;
        }

        if (typeof raw !== "string" || raw.length === 0) {
            return;
        }

        var parts = raw.split("|");
        current.theme = permitted(THEMES, parts[0]);
        current.contrast = permitted(CONTRASTS, parts[1]);
        current.density = permitted(DENSITIES, parts[2]);
    }

    function writeRecord() {
        try {
            window.localStorage.setItem(
                RECORD_KEY,
                (current.theme || "") + "|" + (current.contrast || "") + "|" + (current.density || ""));
        } catch (e) {
            /* Not being able to cache the appearance costs one repaint on the
               next load. It must never cost the operator this load. */
        }
    }

    function prefersLight() {
        try {
            return typeof window.matchMedia === "function"
                && window.matchMedia("(prefers-color-scheme: light)").matches;
        } catch (e) {
            return false;
        }
    }

    function setAttribute(element, name, value) {
        if (!element) {
            return;
        }
        if (value) {
            element.setAttribute(name, value);
        } else {
            element.removeAttribute(name);
        }
    }

    /* Following the system is resolved here rather than left to the stylesheets:
       the token layer deliberately declares no prefers-color-scheme query, so the
       dark palette is what an absent data-theme means. Resolving it explicitly is
       also what lets the desktop head override the answer with its own
       application theme. */
    function resolvedTheme() {
        return current.theme || (prefersLight() ? "light" : "dark");
    }

    function stampBody() {
        setAttribute(document.body, "data-lx-density", current.density);
    }

    function applyDocument() {
        var root = document.documentElement;
        setAttribute(root, "data-theme", resolvedTheme());
        setAttribute(root, "data-contrast", current.contrast);
        stampBody();
    }

    /*
      Called by the application once its durable preferences have loaded, and
      again on every change. Each argument is the attribute value for its axis, or
      null/undefined to follow the environment on it.
    */
    function apply(theme, contrast, density) {
        current.theme = permitted(THEMES, theme);
        current.contrast = permitted(CONTRASTS, contrast);
        current.density = permitted(DENSITIES, density);
        writeRecord();
        applyDocument();
    }

    function watchSystemTheme() {
        var query;
        try {
            if (typeof window.matchMedia !== "function") {
                return;
            }
            query = window.matchMedia("(prefers-color-scheme: light)");
        } catch (e) {
            return;
        }

        var onChange = function () {
            /* Only an operator who has not pinned a palette follows the system. */
            if (!current.theme) {
                setAttribute(document.documentElement, "data-theme", resolvedTheme());
            }
        };

        if (typeof query.addEventListener === "function") {
            query.addEventListener("change", onChange);
        } else if (typeof query.addListener === "function") {
            query.addListener(onChange);
        }
    }

    readRecord();
    applyDocument();
    watchSystemTheme();

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", stampBody);
    }

    window.latticeAppearance = {
        apply: apply,
        stamp: stampBody
    };
})();
