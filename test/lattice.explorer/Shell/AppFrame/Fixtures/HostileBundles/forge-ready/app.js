// Hostile: forge the bootstrap handshake. The host accepts one lattice.ready per frame and
// transfers exactly one port, so none of these may produce a second lattice.hello.
lattice.ready.then(async () => {
  for (let i = 0; i < 5; i += 1) {
    window.parent.postMessage({ type: 'lattice.ready', protocol: 1 }, '*');
  }
  await lattice.request('ui.notify', { text: 'forged-ready-sent' });
});
