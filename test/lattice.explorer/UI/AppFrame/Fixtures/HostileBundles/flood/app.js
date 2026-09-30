// Hostile: exceed the per-frame token bucket (capacity 20) and concurrency limit (4). The
// host must answer the excess with rate_limited and keep serving the frame afterwards.
lattice.ready.then(async () => {
  const results = await Promise.allSettled(
    Array.from({ length: 100 }, () => lattice.request('context.read', {})));
  const limited = results.filter((r) => r.status === 'rejected' && r.reason && r.reason.code === 'rate_limited').length;
  await new Promise((resolve) => setTimeout(resolve, 2500));
  await lattice.request('ui.notify', { text: 'flood ok=' + (100 - limited) + ' limited=' + limited });
});
