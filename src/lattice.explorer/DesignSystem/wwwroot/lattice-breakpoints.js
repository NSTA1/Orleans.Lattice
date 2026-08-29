/*
  Orleans.Lattice Explorer - breakpoint observer.

  Reports the current named breakpoint to .NET and calls back whenever it
  changes. The boundary widths are passed in from `LatticeBreakpoints`, so this
  module holds no breakpoint value of its own: there is exactly one source of
  those numbers on the .NET side and one in `lattice-breakpoints.css`, and the
  hygiene guard keeps the two in step.

  `matchMedia` is used rather than a resize listener so the browser only calls
  back when a boundary is actually crossed - a drag that resizes the window
  within one band costs nothing.
*/

const observers = new Map();
let nextHandle = 1;

function classify(mediumQuery, expandedQuery) {
    if (expandedQuery.matches) {
        return "expanded";
    }

    return mediumQuery.matches ? "medium" : "compact";
}

/**
 * Starts observing the viewport.
 *
 * @param {object} dotNetRef A DotNetObjectReference exposing OnBreakpointChanged.
 * @param {number} mediumMinimumWidth The medium band's inclusive minimum width in CSS pixels.
 * @param {number} expandedMinimumWidth The expanded band's inclusive minimum width in CSS pixels.
 * @returns {{handle: number, breakpoint: string}} The disposal handle and the breakpoint in effect now.
 */
export function observe(dotNetRef, mediumMinimumWidth, expandedMinimumWidth) {
    const mediumQuery = window.matchMedia(`(min-width: ${mediumMinimumWidth}px)`);
    const expandedQuery = window.matchMedia(`(min-width: ${expandedMinimumWidth}px)`);

    let last = classify(mediumQuery, expandedQuery);

    const onChange = () => {
        const current = classify(mediumQuery, expandedQuery);
        if (current === last) {
            return;
        }

        last = current;
        dotNetRef.invokeMethodAsync("OnBreakpointChanged", current);
    };

    mediumQuery.addEventListener("change", onChange);
    expandedQuery.addEventListener("change", onChange);

    const handle = nextHandle++;
    observers.set(handle, () => {
        mediumQuery.removeEventListener("change", onChange);
        expandedQuery.removeEventListener("change", onChange);
    });

    return { handle, breakpoint: last };
}

/**
 * Stops an observation started by `observe` and releases its listeners.
 *
 * @param {number} handle The handle returned by `observe`.
 */
export function release(handle) {
    const dispose = observers.get(handle);
    if (dispose) {
        dispose();
        observers.delete(handle);
    }
}
