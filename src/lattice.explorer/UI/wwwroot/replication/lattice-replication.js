/*
 * Orleans.Lattice Explorer - the Replication area's module.
 *
 * One small thing script does that markup cannot: report whether the document
 * is visible (the Page Visibility API), so a tree's detail page stops its
 * refresh cadence while the tab is in the background and resumes on return.
 *
 * Nothing here is security state, and the area works without it.
 * Plain ASCII only: the repository's hygiene gates scan this file.
 */

export function observeVisibility(target) {
  const report = () => target.invokeMethodAsync('OnVisibilityChanged', document.visibilityState !== 'hidden');

  document.addEventListener('visibilitychange', report);
  report();

  return {
    dispose: () => document.removeEventListener('visibilitychange', report),
  };
}
