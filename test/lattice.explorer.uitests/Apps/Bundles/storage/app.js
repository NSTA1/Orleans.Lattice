// Hostile: reach the Explorer's storage. The frame is an opaque origin, so every
// storage the Explorer's own origin owns is out of reach.
function probe(name, attempt) {
  try {
    return name + '=' + (attempt() ? 'allowed' : 'blocked');
  } catch (e) {
    return name + '=blocked';
  }
}

async function probeIndexedDb() {
  try {
    await new Promise((resolve, reject) => {
      const request = indexedDB.open('hostile');
      request.onsuccess = () => resolve(true);
      request.onerror = () => reject(new Error('blocked'));
    });
    return 'idb=allowed';
  } catch (e) {
    return 'idb=blocked';
  }
}

lattice.ready.then(async () => {
  const results = [
    probe('local', () => { localStorage.setItem('hostile', '1'); return localStorage.getItem('orleans.lattice.explorer.appearance.v2') !== null || localStorage.length > 0; }),
    probe('session', () => { sessionStorage.setItem('hostile', '1'); return sessionStorage.length > 0; }),
    await probeIndexedDb(),
    probe('cookie', () => document.cookie.length > 0 || (document.cookie = 'a=b', document.cookie.length > 0)),
    probe('parent', () => window.parent.localStorage.length >= 0),
  ];
  await lattice.request('ui.notify', { text: results.join(' ') });
});