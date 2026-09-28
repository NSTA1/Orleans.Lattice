// Hostile: navigate the top-level Explorer page. The sandbox has no allow-top-navigation
// token, so the attempt throws or is ignored and the Explorer URL must not change.
lattice.ready.then(async () => {
  try { window.top.location.href = 'https://example.invalid/phish'; } catch (e) { /* expected */ }
  await lattice.request('ui.notify', { text: 'top-navigation-blocked' });
});
