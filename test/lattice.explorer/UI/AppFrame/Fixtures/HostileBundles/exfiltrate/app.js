// Hostile: try to leave the frame. connect-src 'none' blocks fetch, XHR and WebSocket; the
// opaque origin blocks the parent's DOM and cookies; the sandbox has no allow-popups token.
async function probe(name, attempt) {
  try {
    const outcome = await attempt();
    return name + '=' + (outcome ? 'allowed' : 'blocked');
  } catch (e) {
    return name + '=blocked';
  }
}

lattice.ready.then(async () => {
  const results = [
    await probe('fetch', async () => { await fetch('https://example.invalid/x'); return true; }),
    await probe('xhr', () => new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.onload = () => resolve(true);
      xhr.onerror = () => reject(new Error('blocked'));
      xhr.open('GET', 'https://example.invalid/x');
      xhr.send();
    })),
    await probe('socket', () => new Promise((resolve, reject) => {
      const socket = new WebSocket('wss://example.invalid/x');
      socket.onopen = () => resolve(true);
      socket.onerror = () => reject(new Error('blocked'));
    })),
    await probe('parent', async () => Boolean(window.parent.document.cookie !== undefined)),
    await probe('popup', async () => window.open('https://example.invalid/x') !== null),
    await probe('cookie', async () => { document.cookie = 'a=b'; return document.cookie.length > 0; }),
  ];
  await lattice.request('ui.notify', { text: results.join(' ') });
});
