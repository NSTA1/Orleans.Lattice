// Hostile: navigate the frame itself to a page of the Explorer's own origin that is not
// the frame bootstrap. The Explorer refuses to be framed anywhere but the bootstrap path,
// so that page must never run, and the frame host must treat the navigation as the frame
// leaving: it closes the port.
lattice.ready.then(() => {
  location.href = '/uitest/escape';
});