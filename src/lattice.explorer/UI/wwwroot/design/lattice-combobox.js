/*
 * Orleans.Lattice Explorer - the combobox's module.
 *
 * Two small things script does better than markup, both enhancements: the
 * combobox works without them.
 *   - Enter on a highlighted suggestion chooses it. Without this, the same key
 *     would also submit the surrounding form. A field that commits typed text
 *     on Enter (data-lt-enter="commit", or "add" while it holds text) keeps
 *     Enter from submitting the form as well.
 *   - The highlighted suggestion is scrolled into view as the arrow keys move it.
 *
 * Nothing here is security state. Plain ASCII only: the repository's hygiene
 * gates scan this file.
 */

function keepsEnter(input) {
  if (input.getAttribute('aria-expanded') === 'true' && input.getAttribute('aria-activedescendant')) {
    return true;
  }

  const mode = input.getAttribute('data-lt-enter');
  return mode === 'commit' || (mode === 'add' && input.value.trim().length > 0);
}

export function attach(input) {
  if (!input) {
    return null;
  }

  const onKeyDown = (event) => {
    if (event.key === 'Enter' && !event.isComposing && keepsEnter(input)) {
      event.preventDefault();
    }
  };

  const reveal = () => {
    const id = input.getAttribute('aria-activedescendant');
    const option = id ? document.getElementById(id) : null;
    if (option && typeof option.scrollIntoView === 'function') {
      option.scrollIntoView({ block: 'nearest' });
    }
  };

  input.addEventListener('keydown', onKeyDown);
  const observer = typeof MutationObserver === 'undefined'
    ? null
    : new MutationObserver(reveal);
  if (observer) {
    observer.observe(input, { attributes: true, attributeFilter: ['aria-activedescendant'] });
  }

  return {
    dispose: () => {
      input.removeEventListener('keydown', onKeyDown);
      if (observer) {
        observer.disconnect();
      }
    },
  };
}
