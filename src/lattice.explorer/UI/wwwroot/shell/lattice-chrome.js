/*
 * Orleans.Lattice Explorer - the navigation chrome's module.
 *
 * Three small things script does better than markup:
 *   - the global "/" and Ctrl+K (Cmd+K) shortcuts that open the address line;
 *   - putting the chosen appearance on the document element, following the
 *     operating system's light or dark preference live when the operator chose
 *     "System", and remembering it for the first-paint script;
 *   - focusing and selecting the address input, and focusing the chrome's
 *     controls - only while they are still in the document, so a focus request
 *     that lands after a render removed its element is a no-op;
 *   - reporting which width band the Shell root is in, so .NET can render the
 *     compact chrome without any stylesheet naming a width.
 *
 * Nothing here is security state, and the chrome works without it.
 * Plain ASCII only: the repository's hygiene gates scan this file.
 */

const firstPaintKey = 'orleans.lattice.explorer.appearance.v2';
const darkQuery = '(prefers-color-scheme: dark)';

let followSystem = null;

function isTextField(element) {
  if (!element) {
    return false;
  }

  const tag = element.tagName;
  return element.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
}

export function registerShortcuts(target) {
  const onKeyDown = (event) => {
    if (event.defaultPrevented || event.isComposing) {
      return;
    }

    const commandK = (event.ctrlKey || event.metaKey) && !event.altKey && (event.key === 'k' || event.key === 'K');
    const slash = event.key === '/' && !event.ctrlKey && !event.metaKey && !event.altKey && !isTextField(event.target);

    if (commandK || slash) {
      event.preventDefault();
      target.invokeMethodAsync('OpenAddressLine');
    }
  };

  document.addEventListener('keydown', onKeyDown);

  return {
    dispose: () => document.removeEventListener('keydown', onKeyDown),
  };
}

function resolveTheme(theme) {
  if (theme === 'light' || theme === 'dark') {
    return theme;
  }

  return window.matchMedia && window.matchMedia(darkQuery).matches ? 'dark' : 'light';
}

function setAttributes(theme, contrast, density) {
  const root = document.documentElement;
  root.setAttribute('data-bs-theme', resolveTheme(theme));

  if (contrast === 'standard' || contrast === 'more') {
    root.setAttribute('data-lt-contrast', contrast);
  } else {
    root.removeAttribute('data-lt-contrast');
  }

  if (density === 'compact') {
    root.setAttribute('data-lt-density', 'compact');
  } else {
    root.removeAttribute('data-lt-density');
  }
}

export function applyAppearance(theme, contrast, density) {
  setAttributes(theme, contrast, density);

  if (followSystem) {
    followSystem.query.removeEventListener('change', followSystem.listener);
    followSystem = null;
  }

  if (theme !== 'light' && theme !== 'dark' && window.matchMedia) {
    const query = window.matchMedia(darkQuery);
    const listener = () => setAttributes(theme, contrast, density);
    query.addEventListener('change', listener);
    followSystem = { query, listener };
  }

  try {
    window.localStorage.setItem(firstPaintKey, JSON.stringify({ theme, contrast, density }));
  } catch {
    // Storage may be unavailable (a private window with storage disabled).
  }
}

export function focusAndSelect(element) {
  if (element && element.isConnected) {
    element.focus();
    if (typeof element.select === 'function') {
      element.select();
    }
  }
}

// Focuses an element only if it is still in the document. A focus request can land
// after a render has removed its element; that must cost the focus, never the circuit.
export function focusElement(element) {
  if (element && element.isConnected) {
    element.focus();
  }
}

// Reports the band the element's inline size falls in whenever it changes. The
// band edges are passed in from .NET, the one place besides the breakpoint
// stylesheet that names them.
export function observeViewport(element, target, edges) {
  if (!element || typeof ResizeObserver === 'undefined') {
    return null;
  }

  let band = -1;
  const report = (size) => {
    let next = 0;
    for (const edge of edges) {
      if (size >= edge) {
        next++;
      }
    }

    if (next !== band) {
      band = next;
      target.invokeMethodAsync('OnViewportBand', band);
    }
  };

  const observer = new ResizeObserver((entries) => {
    for (const entry of entries) {
      const box = entry.contentBoxSize && entry.contentBoxSize[0];
      report(box ? box.inlineSize : entry.contentRect.width);
    }
  });

  observer.observe(element);
  report(element.getBoundingClientRect().width);

  return {
    dispose: () => observer.disconnect(),
  };
}
