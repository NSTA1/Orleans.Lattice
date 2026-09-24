// Orleans.Lattice documentation site - behaviour layered over DocFX's modern
// template. DocFX imports this module and reads the exported options; start()
// runs before the template renders its navigation, so anything that needs the
// rendered DOM waits for window.docfx.ready.
//
// Everything here is progressive: with this file absent the site still works,
// it only loses the enhancements below. Plain ASCII only (hygiene gates).

const REPO = 'https://github.com/NSTA1/Orleans.Lattice';

const MARK_SVG =
  '<svg class="lt-mark" viewBox="0 0 32 32" aria-hidden="true" focusable="false">' +
  '<path class="lt-mark-edge" d="M16 27.5 L5.5 16 M16 27.5 L26.5 16 M5.5 16 L16 4.5 M26.5 16 L16 4.5" fill="none" stroke-width="2" stroke-linecap="round"/>' +
  '<circle class="lt-mark-node" cx="5.5" cy="16" r="3.3"/>' +
  '<circle class="lt-mark-node" cx="26.5" cy="16" r="3.3"/>' +
  '<circle class="lt-mark-bottom" cx="16" cy="27.5" r="3.1" stroke-width="1.8"/>' +
  '<circle class="lt-mark-join" cx="16" cy="4.5" r="4" stroke-width="1.8"/>' +
  '</svg>';

const reducedMotion = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const isDark = () => document.documentElement.getAttribute('data-bs-theme') === 'dark';

function whenRendered(fn) {
  const started = Date.now();
  (function poll() {
    if (window.docfx && window.docfx.ready) { fn(); return; }
    if (Date.now() - started > 15000) { fn(); return; }
    setTimeout(poll, 40);
  })();
}

// --- Skip link -----------------------------------------------------------

function addSkipLink() {
  const main = document.querySelector('body > main');
  if (!main || document.querySelector('.lt-skip')) return;
  main.id = main.id || 'lt-main';
  main.setAttribute('tabindex', '-1');
  const skip = document.createElement('a');
  skip.className = 'lt-skip';
  skip.href = '#' + main.id;
  skip.textContent = 'Skip to content';
  document.body.insertBefore(skip, document.body.firstChild);
}

// --- Brand: an inline mark that follows the theme, and a quieter "Orleans." --

function enhanceBrand() {
  const brand = document.querySelector('.navbar-brand');
  if (!brand || brand.dataset.ltBrand) return;
  brand.dataset.ltBrand = '1';
  const logo = brand.querySelector('#logo');
  if (logo) {
    logo.insertAdjacentHTML('afterend', MARK_SVG);
    logo.remove();
  }
  for (const node of Array.from(brand.childNodes)) {
    if (node.nodeType === Node.TEXT_NODE && node.textContent.includes('Orleans.Lattice')) {
      const name = document.createElement('span');
      name.className = 'lt-brand-name';
      name.innerHTML = '<span class="lt-brand-org">Orleans.</span>Lattice';
      node.replaceWith(name);
    }
  }
  brand.setAttribute('aria-label', 'Orleans.Lattice documentation home');
}

// --- Search: "/" focuses it, as on GitHub -----------------------------------

function enhanceSearch() {
  const input = document.getElementById('search-query');
  if (!input || input.dataset.ltSearch) return;
  input.dataset.ltSearch = '1';
  input.setAttribute('placeholder', 'Search the docs');
  const hint = document.createElement('kbd');
  hint.className = 'lt-kbd-hint';
  hint.textContent = '/';
  hint.setAttribute('aria-hidden', 'true');
  input.insertAdjacentElement('afterend', hint);
  input.setAttribute('aria-keyshortcuts', '/');

  document.addEventListener('keydown', event => {
    if (event.key !== '/' || event.ctrlKey || event.metaKey || event.altKey) return;
    const target = event.target;
    const typing = target instanceof HTMLElement &&
      (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName));
    if (typing || input.disabled) return;
    event.preventDefault();
    input.focus();
    input.select();
  });
}

// --- Tables: wide ones scroll in their own frame ------------------------------

function wrapTables() {
  const tables = document.querySelectorAll('article table');
  tables.forEach(table => {
    if (table.parentElement && table.parentElement.classList.contains('lt-table-scroll')) return;
    const frame = document.createElement('div');
    frame.className = 'lt-table-scroll';
    table.parentNode.insertBefore(frame, table);
    frame.appendChild(table);
  });

  const mark = () => {
    document.querySelectorAll('.lt-table-scroll').forEach(frame => {
      const overflowing = frame.scrollWidth > frame.clientWidth + 1;
      if (overflowing) {
        frame.setAttribute('tabindex', '0');
        frame.setAttribute('role', 'region');
        frame.setAttribute('aria-label', 'Table, scrolls horizontally');
      } else {
        frame.removeAttribute('tabindex');
        frame.removeAttribute('role');
        frame.removeAttribute('aria-label');
      }
    });
  };
  mark();
  let pending = 0;
  window.addEventListener('resize', () => {
    cancelAnimationFrame(pending);
    pending = requestAnimationFrame(mark);
  });
}

// --- Sidebar: open with the current page in view, not at the scroll edge --------

function centreCurrentPage() {
  const current = document.querySelector('#toc li.active:not(.expander) > a');
  const scroller = current && current.closest('.overflow-y-auto');
  if (!scroller) return;
  const top = current.getBoundingClientRect().top - scroller.getBoundingClientRect().top + scroller.scrollTop;
  scroller.scrollTop = Math.max(0, top - scroller.clientHeight / 3);
}

// --- In this article: mark the section in view --------------------------------

function trackSections() {
  const affix = document.getElementById('affix');
  if (!affix) return;
  const links = Array.from(affix.querySelectorAll('a[href^="#"]'));
  if (links.length === 0) return;

  const byId = new Map();
  const headings = [];
  for (const link of links) {
    const id = decodeURIComponent(link.getAttribute('href').slice(1));
    const heading = document.getElementById(id);
    if (!heading) continue;
    byId.set(heading, link);
    headings.push(heading);
  }
  if (headings.length === 0) return;

  const scroller = affix.closest('.affix');
  let current = null;

  const update = () => {
    const offset = 96;
    let active = headings[0];
    for (const heading of headings) {
      if (heading.getBoundingClientRect().top - offset <= 0) active = heading;
      else break;
    }
    const atEnd = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4;
    if (atEnd) active = headings[headings.length - 1];
    if (active === current) return;
    current = active;
    let passed = true;
    for (const heading of headings) {
      const link = byId.get(heading);
      const isCurrent = heading === active;
      if (isCurrent) passed = false;
      link.classList.toggle('lt-current', isCurrent);
      link.classList.toggle('lt-passed', passed && !isCurrent);
      if (isCurrent) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    }
    const link = byId.get(active);
    if (scroller && link) {
      const top = link.offsetTop;
      const view = scroller.scrollTop;
      const height = scroller.clientHeight;
      if (top < view + 40 || top > view + height - 60) {
        scroller.scrollTop = Math.max(0, top - height / 3);
      }
    }
  };

  let ticking = false;
  window.addEventListener('scroll', () => {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(() => { ticking = false; update(); });
  }, { passive: true });
  update();
}

// --- Join figures: the animated order diagrams ---------------------------------
// Every figure - the home page's, and one per CRDT explainer - is generated by
// stage.ps1 from docs-site/figures/join-figures.json, and each token carries its
// own route, so nothing here knows about any one CRDT. Phase one is the two
// concurrent writes; phase two is each cluster merging the other's delta, which
// lands both on the join. Redelivery sends one delta again to show that merging
// what the join already holds changes nothing. The figure is complete without
// this: it shows the converged state, and reduced motion keeps it there.

const PLAY_MS = 2600;
const REDELIVER_MS = 1400;
const FRAMES_PER_PHASE = 18;

// The same curve the edges draw with in CSS, so a token and its edge arrive together.
function cubicBezier(x1, y1, x2, y2) {
  const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx;
  const cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
  const curveX = t => ((ax * t + bx) * t + cx) * t;
  const curveY = t => ((ay * t + by) * t + cy) * t;
  const slopeX = t => (3 * ax * t + 2 * bx) * t + cx;
  return x => {
    let t = x;
    for (let i = 0; i < 8; i++) {
      const error = curveX(t) - x;
      if (Math.abs(error) < 1e-5) return curveY(t);
      const slope = slopeX(t);
      if (Math.abs(slope) < 1e-6) break;
      t -= error / slope;
    }
    let low = 0, high = 1;
    t = x;
    for (let i = 0; i < 24; i++) {
      if (curveX(t) < x) low = t; else high = t;
      t = (low + high) / 2;
    }
    return curveY(t);
  };
}

const easeMerge = cubicBezier(0.45, 0, 0.2, 1);

const pointOn = (from, via, to, t) => {
  if (!via) return [from[0] + (to[0] - from[0]) * t, from[1] + (to[1] - from[1]) * t];
  const u = 1 - t;
  return [
    u * u * from[0] + 2 * u * t * via[0] + t * t * to[0],
    u * u * from[1] + 2 * u * t * via[1] + t * t * to[1]
  ];
};

const moveTo = p => 'translate(' + p[0] + 'px, ' + p[1] + 'px)';

// A route is { start, phases }: each phase eases the token to its 'to' point,
// along a curve when it names a 'via', or holds it where it is when empty.
function routeFrames(route) {
  const phases = route.phases || [];
  const share = 1 / Math.max(1, phases.length);
  let at = route.start;
  const frames = [{ offset: 0, transform: moveTo(at) }];
  phases.forEach((phase, index) => {
    const base = index * share;
    if (!phase || !phase.to) {
      frames.push({ offset: Math.min(1, base + share), transform: moveTo(at) });
      return;
    }
    const from = at;
    for (let k = 1; k <= FRAMES_PER_PHASE; k++) {
      const progress = k / FRAMES_PER_PHASE;
      frames.push({
        offset: Math.min(1, base + share * progress),
        transform: moveTo(pointOn(from, phase.via, phase.to, easeMerge(progress)))
      });
    }
    at = phase.to;
  });
  return frames;
}

const FADE = [{ opacity: 0 }, { opacity: 1, offset: 0.05 }, { opacity: 1, offset: 0.95 }, { opacity: 0 }];

function setUpJoinFigure(figure) {
  if (figure.dataset.ltReady) return;
  figure.dataset.ltReady = '1';

  const text = figure.dataset;
  const status = figure.querySelector('[data-lt-join-status]');
  const replay = figure.querySelector('[data-lt-join-replay]');
  const redeliver = figure.querySelector('[data-lt-join-redeliver]');
  const tokens = Array.from(figure.querySelectorAll('[data-lt-token]'));
  const writers = tokens.filter(token => token.dataset.ltToken !== 'dup');
  const duplicate = tokens.find(token => token.dataset.ltToken === 'dup');

  const say = message => { if (status && message) status.textContent = message; };

  let running = [];
  let timers = [];
  const stop = () => {
    running.forEach(animation => animation.cancel());
    timers.forEach(clearTimeout);
    running = [];
    timers = [];
    figure.classList.remove('is-arrived');
  };

  const travel = (token, duration) => {
    let route;
    try { route = JSON.parse(token.dataset.ltRoute || '{}'); } catch { return Promise.resolve(); }
    if (!route.start) return Promise.resolve();
    const motion = token.animate(routeFrames(route), { duration, easing: 'linear', fill: 'both' });
    const fade = token.animate(FADE, { duration, easing: 'linear', fill: 'both' });
    running.push(motion, fade);
    return motion.finished;
  };

  const settle = () => {
    stop();
    figure.classList.remove('is-running', 'is-redelivered');
    figure.classList.add('is-joined');
    say(text.ltSettled);
  };

  const play = () => {
    stop();
    figure.classList.remove('is-joined', 'is-redelivered', 'is-running');
    void figure.getBoundingClientRect();
    figure.classList.add('is-running');
    say(text.ltStep1);
    timers.push(setTimeout(() => say(text.ltStep2), PLAY_MS / 2));
    Promise.all(writers.map(token => travel(token, PLAY_MS))).then(() => {
      figure.classList.remove('is-running');
      figure.classList.add('is-joined', 'is-arrived');
      timers.push(setTimeout(() => figure.classList.remove('is-arrived'), 1000));
      say(text.ltSettled);
    }).catch(() => {});
  };

  const again = () => {
    stop();
    figure.classList.remove('is-running', 'is-redelivered');
    figure.classList.add('is-joined');
    if (reducedMotion() || !duplicate) {
      figure.classList.add('is-redelivered');
      say(text.ltReduced);
      return;
    }
    say(text.ltPending);
    travel(duplicate, REDELIVER_MS).then(() => {
      figure.classList.add('is-redelivered');
      say(text.ltDone);
    }).catch(() => {});
  };

  if (replay) replay.addEventListener('click', () => (reducedMotion() ? settle() : play()));
  if (redeliver) redeliver.addEventListener('click', again);

  settle();

  // Each figure plays once, the first time most of it scrolls into view.
  if (!reducedMotion() && 'IntersectionObserver' in window) {
    const seen = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting)) {
        seen.disconnect();
        timers.push(setTimeout(play, 350));
      }
    }, { threshold: 0.5 });
    seen.observe(figure);
  }
}

function joinFigures() {
  document.querySelectorAll('[data-lt-join]').forEach(setUpJoinFigure);
}

// --- Options DocFX reads ---------------------------------------------------------

export default {
  defaultTheme: 'auto',

  iconLinks: [
    { icon: 'github', href: REPO, title: 'Orleans.Lattice on GitHub' }
  ],

  // Read at render time, so a theme switch re-renders diagrams in the right ink.
  get mermaid() {
    const dark = isDark();
    const ink = dark ? '#e4e9e5' : '#15191f';
    const ink2 = dark ? '#a8b3ac' : '#4b5361';
    const surface = dark ? '#101613' : '#ffffff';
    const sunken = dark ? '#151c18' : '#f4f5f7';
    const rule = dark ? '#3a4640' : '#c3c9d1';
    const note = dark ? '#2a2a17' : '#fff1bf';
    return {
      theme: 'base',
      darkMode: dark,
      fontFamily: '"Recursive Sans Linear", "Segoe UI", system-ui, sans-serif',
      themeVariables: {
        darkMode: dark,
        fontFamily: '"Recursive Sans Linear", "Segoe UI", system-ui, sans-serif',
        fontSize: '14px',
        background: surface,
        mainBkg: sunken,
        primaryColor: sunken,
        primaryTextColor: ink,
        primaryBorderColor: ink2,
        secondaryColor: surface,
        secondaryTextColor: ink,
        secondaryBorderColor: rule,
        tertiaryColor: surface,
        tertiaryTextColor: ink,
        tertiaryBorderColor: rule,
        nodeBorder: ink2,
        nodeTextColor: ink,
        lineColor: ink2,
        textColor: ink,
        titleColor: ink,
        edgeLabelBackground: surface,
        clusterBkg: surface,
        clusterBorder: rule,
        noteBkgColor: note,
        noteTextColor: ink,
        noteBorderColor: rule,
        actorBkg: sunken,
        actorBorder: ink2,
        actorTextColor: ink,
        actorLineColor: rule,
        signalColor: ink2,
        signalTextColor: ink,
        labelBoxBkgColor: sunken,
        labelBoxBorderColor: rule,
        labelTextColor: ink,
        loopTextColor: ink,
        activationBkgColor: surface,
        activationBorderColor: ink2,
        sequenceNumberColor: surface
      }
    };
  },

  start() {
    addSkipLink();
    if (document.fonts && document.fonts.load) {
      document.fonts.load('400 1rem "Recursive Sans Linear"').catch(() => {});
    }
    whenRendered(() => {
      enhanceBrand();
      enhanceSearch();
      wrapTables();
      centreCurrentPage();
      trackSections();
      joinFigures();
    });
  }
};
