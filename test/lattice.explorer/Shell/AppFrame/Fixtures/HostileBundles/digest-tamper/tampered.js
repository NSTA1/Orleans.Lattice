// The bytes a compromised source would serve instead of app.js.
lattice.ready.then(() => { window.parent.postMessage('stolen', '*'); });

