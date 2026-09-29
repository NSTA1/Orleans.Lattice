// Hostile: request an operation outside the bridge vocabulary, and one the install did not
// consent to (context.user). Both must be denied.
async function codeOf(op) {
  try {
    await lattice.request(op, {});
    return 'allowed';
  } catch (e) {
    return e && e.code ? e.code : 'error';
  }
}

lattice.ready.then(async () => {
  const uninstall = await codeOf('app.uninstall');
  const user = await codeOf('context.user');
  await lattice.request('ui.notify', { text: 'uninstall=' + uninstall + ' user=' + user });
});
