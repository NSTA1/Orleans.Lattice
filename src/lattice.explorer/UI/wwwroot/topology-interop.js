// Pan/zoom interaction for the topology SVG canvas. Blazor renders a static
// radial graph with a fitted viewBox; this module wires wheel-to-zoom (toward
// the cursor) and pointer-drag-to-pan by mutating the SVG's viewBox directly,
// so interaction never round-trips through Blazor.

function readViewBox(svg) {
    const v = svg.viewBox.baseVal;
    return { x: v.x, y: v.y, w: v.width, h: v.height };
}

function writeViewBox(svg, b) {
    svg.setAttribute('viewBox', `${b.x} ${b.y} ${b.w} ${b.h}`);
}

const ZOOM_STEP = 0.85;
const MAX_ZOOM_OUT = 8;   // viewBox may grow up to 8x the fitted home
const MAX_ZOOM_IN = 0.08; // ...and shrink down to 8% of it

export function attach(svg) {
    if (!svg || svg.dataset.tpzAttached === '1') {
        return;
    }

    svg.dataset.tpzAttached = '1';
    svg._tpzHome = readViewBox(svg);

    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    let moved = 0;

    svg.addEventListener('wheel', (e) => {
        e.preventDefault();

        const b = readViewBox(svg);
        const rect = svg.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
            return;
        }

        const fx = (e.clientX - rect.left) / rect.width;
        const fy = (e.clientY - rect.top) / rect.height;
        const px = b.x + fx * b.w;
        const py = b.y + fy * b.h;

        const factor = e.deltaY < 0 ? ZOOM_STEP : 1 / ZOOM_STEP;
        const home = svg._tpzHome;
        let nw = b.w * factor;
        let nh = b.h * factor;
        nw = Math.min(home.w * MAX_ZOOM_OUT, Math.max(home.w * MAX_ZOOM_IN, nw));
        nh = Math.min(home.h * MAX_ZOOM_OUT, Math.max(home.h * MAX_ZOOM_IN, nh));

        writeViewBox(svg, { x: px - fx * nw, y: py - fy * nh, w: nw, h: nh });
    }, { passive: false });

    svg.addEventListener('pointerdown', (e) => {
        dragging = true;
        moved = 0;
        lastX = e.clientX;
        lastY = e.clientY;
        svg.setPointerCapture(e.pointerId);
        svg.classList.add('is-panning');
    });

    svg.addEventListener('pointermove', (e) => {
        if (!dragging) {
            return;
        }

        const b = readViewBox(svg);
        const rect = svg.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
            return;
        }

        const dx = (e.clientX - lastX) / rect.width * b.w;
        const dy = (e.clientY - lastY) / rect.height * b.h;
        moved += Math.abs(e.clientX - lastX) + Math.abs(e.clientY - lastY);
        lastX = e.clientX;
        lastY = e.clientY;
        writeViewBox(svg, { x: b.x - dx, y: b.y - dy, w: b.w, h: b.h });
    });

    const end = (e) => {
        if (!dragging) {
            return;
        }

        dragging = false;
        svg.classList.remove('is-panning');
        try {
            svg.releasePointerCapture(e.pointerId);
        } catch {
            // pointer may already be released
        }

        // Swallow the click that follows a real drag so it does not trigger a
        // node expand.
        if (moved > 5) {
            const swallow = (ev) => {
                ev.stopPropagation();
                ev.preventDefault();
                svg.removeEventListener('click', swallow, true);
            };
            svg.addEventListener('click', swallow, true);
        }
    };

    svg.addEventListener('pointerup', end);
    svg.addEventListener('pointercancel', end);
    svg.addEventListener('dblclick', (e) => {
        e.preventDefault();
        reset(svg);
    });
}

// Re-capture the current viewBox as the "home" frame (called after Blazor
// re-fits the canvas for a new or expanded graph).
export function home(svg) {
    if (!svg) {
        return;
    }

    svg._tpzHome = readViewBox(svg);
}

// Restore the canvas to its fitted home frame.
export function reset(svg) {
    if (!svg || !svg._tpzHome) {
        return;
    }

    writeViewBox(svg, svg._tpzHome);
}
