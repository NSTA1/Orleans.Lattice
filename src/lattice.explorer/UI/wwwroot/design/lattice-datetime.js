/*
 * Orleans.Lattice Explorer - the date and time field's module.
 *
 * Two small things script does better than markup, both enhancements: the field
 * works without them.
 *   - zone() reports the reader's time zone, so the field can show the local
 *     time beside the UTC one. The field never converts anything with it.
 *   - In the calendar grid, the arrow, page, Home and End keys move the focused
 *     day; this keeps them from also scrolling the page.
 *
 * Nothing here is security state. Plain ASCII only: the repository's hygiene
 * gates scan this file.
 */

const MOVES = new Set(['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', 'Home', 'End']);

export function zone() {
  const offsetMinutes = -new Date().getTimezoneOffset();
  try {
    const id = Intl.DateTimeFormat().resolvedOptions().timeZone;
    return { id: id || null, offsetMinutes };
  } catch {
    return { id: null, offsetMinutes };
  }
}

export function attach(root) {
  if (!root) {
    return null;
  }

  const onKeyDown = (event) => {
    const target = event.target;
    if (MOVES.has(event.key) && target && typeof target.closest === 'function' && target.closest('[role="grid"]')) {
      event.preventDefault();
    }
  };

  root.addEventListener('keydown', onKeyDown);
  return {
    dispose: () => root.removeEventListener('keydown', onKeyDown),
  };
}
