// Hostile: reload the frame's own document once the kit is ready. The host must close the
// port, tear the frame down and show its Reloaded state.
lattice.ready.then(() => { location.reload(); });
