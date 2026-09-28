/*
 * Orleans.Lattice Explorer - the first-paint appearance script.
 *
 * A classic, blocking script for <head>: it must not gain defer or async, and
 * must not become a module, or the remembered material arrives after the first
 * paint and every load flashes the wrong one. It reads only the small, non-secret
 * record the chrome module writes (theme, contrast and density names), accepts
 * only the names the Explorer ships, and otherwise follows the operating system.
 * The preference contract remains the authority: the chrome re-applies from it
 * as soon as the application starts.
 *
 * Plain ASCII only: the repository's hygiene gates scan this file.
 */
(function () {
  var record = null;
  try {
    record = JSON.parse(window.localStorage.getItem('orleans.lattice.explorer.appearance.v2') || 'null');
  } catch (e) {
    record = null;
  }

  var theme = record && (record.theme === 'light' || record.theme === 'dark') ? record.theme : 'system';
  var contrast = record && (record.contrast === 'standard' || record.contrast === 'more') ? record.contrast : null;
  var compact = !!record && record.density === 'compact';

  if (theme === 'system') {
    theme = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  var root = document.documentElement;
  root.setAttribute('data-bs-theme', theme);
  if (contrast) {
    root.setAttribute('data-lt-contrast', contrast);
  }

  if (compact) {
    root.setAttribute('data-lt-density', 'compact');
  }
})();
