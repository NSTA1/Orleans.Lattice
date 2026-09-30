/*
 * Orleans.Lattice Explorer - the tabs' module.
 *
 * A tab row that is wider than its column scrolls inside its own frame rather
 * than wrapping. This keeps the selected tab in that frame's view, by moving the
 * row alone: it never scrolls the page. It is an enhancement: without it the tabs
 * still work, and keyboard focus scrolls a focused tab into view by itself.
 *
 * Nothing here is security state. Plain ASCII only: the repository's hygiene
 * gates scan this file.
 */

export function reveal(list) {
  if (!list || !list.isConnected) {
    return;
  }

  const tab = list.querySelector('[role="tab"][aria-selected="true"]');
  if (!tab) {
    return;
  }

  const frame = list.getBoundingClientRect();
  const box = tab.getBoundingClientRect();
  if (box.left < frame.left) {
    list.scrollLeft += box.left - frame.left;
  } else if (box.right > frame.right) {
    list.scrollLeft += box.right - frame.right;
  }
}
