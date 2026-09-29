// Hostile: reach a tree by its physical id, and a logical tree the app does not declare.
// Both must be denied, and the host must never forward either to the cluster.
async function codeOf(tree) {
  try {
    await lattice.request('data.read', { action: 'get', tree, key: 'k' });
    return 'allowed';
  } catch (e) {
    return e && e.code ? e.code : 'error';
  }
}

lattice.ready.then(async () => {
  const physical = await codeOf('t/default/a/other/secrets');
  const undeclared = await codeOf('secrets');
  await lattice.request('ui.notify', { text: 'physical=' + physical + ' undeclared=' + undeclared });
});
