// Main-thread shim for the fleet renderer.
//
// All actual drawing happens in `fleet-worker.js`. This file:
//   1. Transfers control of the visible <canvas> to a dedicated Worker via
//      `transferControlToOffscreen()`. After the transfer, the main thread has *no* access to
//      the canvas's drawing context -- everything is owned by the worker.
//   2. Forwards messages between Blazor and the worker (layout, viewport, vehicle packets,
//      dispose).
//   3. Watches the canvas with a ResizeObserver so the worker can keep its WebGL viewport and
//      static-map texture in sync with the on-screen size.
//   4. Receives the worker's computed projection (scale + offsets) and reports it back to
//      Blazor via the supplied DotNetObjectReference, so C# can pre-project vehicle
//      coordinates into device-pixel screen space before sending each packet.
//
// This isolates rendering from the Blazor WASM main-thread event loop. Even when C# is busy
// processing telemetry or running a render-loop tick, the worker's requestAnimationFrame keeps
// firing at the display refresh rate.

const renderers = new Map();

function ensure(canvasId, dotnetRef) {
    let r = renderers.get(canvasId);
    if (r) {
        if (dotnetRef) r.dotnetRef = dotnetRef;
        return r;
    }
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    if (typeof canvas.transferControlToOffscreen !== "function" ||
        typeof OffscreenCanvas === "undefined" ||
        typeof Worker === "undefined") {
        console.error("fleetCanvas: browser missing OffscreenCanvas / Worker support; rendering disabled.");
        return null;
    }
    const offscreen = canvas.transferControlToOffscreen();
    const worker = new Worker("js/fleet-worker.js");
    r = {
        canvasId,
        canvas,
        worker,
        dotnetRef,
        dpr: Math.max(1, window.devicePixelRatio || 1),
        observer: null,
        // Camera state mirrored on the main thread so input handlers can do incremental
        // updates (pan += delta, zoom *= factor) without round-tripping the worker.
        panX: 0,
        panY: 0,
        zoom: 1,
        pitchRad: 0,
        yawRad: 0,
        // Mirrored from setLayout / viewport-proj so we can hit-test cities in canvas-pixel
        // space and convert pointer deltas into graph (layout) units when dragging a city.
        cities: null,
        proj: null,
    };
    worker.onmessage = (e) => onWorkerMessage(r, e.data);
    worker.onerror = (e) => console.error("fleet-worker error:", e.message, e);
    worker.postMessage({ type: "init", canvas: offscreen, dpr: r.dpr }, [offscreen]);
    pushViewport(r);
    r.observer = new ResizeObserver(() => pushViewport(r));
    r.observer.observe(canvas);
    attachInputHandlers(r);
    renderers.set(canvasId, r);
    return r;
}

// Pointer drag has three modes selected by mouse button:
//   * Left-click on a city  -> drag that city in graph space ("city" mode).
//   * Left-click empty space -> pan the camera ("pan" mode).
//   * Right-click anywhere   -> tilt the camera ("tilt" mode): horizontal drag changes yaw,
//                                vertical drag changes pitch. The browser context menu is
//                                suppressed below so the right-button drag isn't interrupted.
// Wheel zooms exponentially. We keep camera state on `r` so concurrent inputs (e.g. user
// wheels mid-drag) accumulate cleanly, and we ignore secondary pointerdowns while a drag is
// already active so a stray button press during a gesture doesn't switch modes mid-flight.
function attachInputHandlers(r) {
    let mode = null; // null | "pan" | "tilt" | "city"
    let cityId = null;
    let lastX = 0;
    let lastY = 0;
    // Radians of camera rotation per CSS pixel of drag. ~0.29°/px keeps a brisk drag (~200 px)
    // well inside the ±60° envelope without feeling sluggish on small adjustments.
    const TILT_RAD_PER_PX = 0.005;
    // Clamp pitch to [0°, 60°] (top-down to oblique) and yaw to [−60°, 60°]. The static-map
    // hit-test approximates rotation as axis-aligned foreshortening, which stays accurate
    // inside this envelope; beyond it the texture would also start showing visible edge-on
    // distortion.
    const PITCH_MIN = 0;
    const PITCH_MAX = Math.PI / 3;
    const YAW_MIN = -Math.PI / 3;
    const YAW_MAX = Math.PI / 3;
    r.canvas.addEventListener("pointerdown", (e) => {
        // Ignore secondary button presses while a drag is in flight: a stray right-click
        // mid-pan would otherwise yank the camera into tilt mode without the user releasing
        // the original button. The first pointerup ends the gesture; the next press starts
        // a fresh one.
        if (mode) return;
        lastX = e.clientX;
        lastY = e.clientY;
        if (e.button === 0) {
            // Left button: city pickup wins on a hit, otherwise pan.
            cityId = hitTestCity(r, e);
            mode = cityId ? "city" : "pan";
            // Closed-hand cursor while a city is being dragged. Pan stays with the default
            // arrow so the empty-canvas drag is visually distinct from a city pickup.
            if (mode === "city") r.canvas.style.cursor = "grabbing";
        } else if (e.button === 2) {
            // Right button: multi-axis tilt regardless of what's under the cursor. We don't
            // hit-test cities here -- right-drag-on-city should still tilt, since picking up
            // a city is the left-click contract.
            cityId = null;
            mode = "tilt";
        } else {
            return;
        }
        try { r.canvas.setPointerCapture(e.pointerId); } catch { }
    });
    r.canvas.addEventListener("pointermove", (e) => {
        if (!mode) {
            // Hover feedback: open-hand cursor when the pointer sits inside a city's
            // (zoom-aware) hit zone, default arrow otherwise. Cheap enough to run on every
            // move because hitTestCity is a linear scan over a few dozen nodes and we only
            // do it while idle.
            const hover = hitTestCity(r, e);
            r.canvas.style.cursor = hover ? "grab" : "";
            return;
        }
        const dxCss = e.clientX - lastX;
        const dyCss = e.clientY - lastY;
        lastX = e.clientX;
        lastY = e.clientY;
        if (mode === "pan") {
            // Convert CSS-pixel pointer delta into world (canvas-pixel) units: world = css *
            // dpr / zoom. The camera mirror is updated immediately so subsequent gestures
            // (e.g. wheel zoom) see the latest pan, and the worker is told the absolute
            // panX/panY rather than a delta so we don't drift if a postMessage is dropped.
            const k = r.dpr / Math.max(0.0001, r.zoom);
            r.panX += dxCss * k;
            r.panY += dyCss * k;
            r.worker.postMessage({ type: "camera", panX: r.panX, panY: r.panY });
        } else if (mode === "tilt") {
            // Drag up tilts the world toward the viewer (pitch ↑); drag right swings the
            // scene clockwise around the up axis (yaw ↑). Both are clamped before any
            // postMessage so the camera mirror and the worker never disagree on bounds.
            const nextPitch = Math.min(PITCH_MAX, Math.max(PITCH_MIN, r.pitchRad - dyCss * TILT_RAD_PER_PX));
            const nextYaw = Math.min(YAW_MAX, Math.max(YAW_MIN, r.yawRad + dxCss * TILT_RAD_PER_PX));
            if (nextPitch === r.pitchRad && nextYaw === r.yawRad) return;
            r.pitchRad = nextPitch;
            r.yawRad = nextYaw;
            r.worker.postMessage({ type: "camera", pitchRad: nextPitch, yawRad: nextYaw });
        } else if (mode === "city" && cityId && r.proj && r.cities) {
            // Convert CSS-pixel pointer delta into a graph-coordinate delta. The composed
            // camera transform applied to a world point (x, y) produces, in device-pixel
            // screen space:
            //   sx = cx + zoom * cos(yaw)  * (x - cx + panX)
            //   sy = cy + zoom * cos(pitch) * (y - cy + panY)
            //                 + zoom * sin(pitch) * sin(yaw) * (x - cx + panX)
            // The naive separable inverse ("divide dx by cos(yaw), dy by cos(pitch)") ignores
            // the sin(pitch)*sin(yaw) cross term and drifts off the dot the moment both axes
            // are non-zero. Inverting the system properly only costs one extra multiply-add:
            //   dx_world_dev = dx_dev / (zoom * cos(yaw))
            //   dy_world_dev = (dy_dev - sin(pitch) * tan(yaw) * dx_dev) / (zoom * cos(pitch))
            // dx_dev / dy_dev are the pointer delta in device pixels (dxCss * dpr).
            const cosP = Math.max(0.0001, Math.cos(r.pitchRad || 0));
            const cosY = Math.max(0.0001, Math.cos(r.yawRad || 0));
            const sinP = Math.sin(r.pitchRad || 0);
            const sinY = Math.sin(r.yawRad || 0);
            const tanY = sinY / cosY;
            const zoom = Math.max(0.0001, r.zoom);
            const dDevX = dxCss * r.dpr;
            const dDevY = dyCss * r.dpr;
            const dWorldX = dDevX / (zoom * cosY);
            const dWorldY = (dDevY - sinP * tanY * dDevX) / (zoom * cosP);
            const dGraphX = dWorldX / r.proj.scale;
            const dGraphY = dWorldY / r.proj.scale;
            const c = r.cities.find((x) => x.id === cityId);
            if (c) {
                c.x += dGraphX;
                c.y += dGraphY;
                r.worker.postMessage({ type: "cityMove", id: cityId, x: c.x, y: c.y });
                if (r.dotnetRef) {
                    r.dotnetRef.invokeMethodAsync("OnCityMoved", cityId, c.x, c.y);
                }
            }
        }
    });
    const endDrag = (e) => {
        if (!mode) return;
        mode = null;
        cityId = null;
        // Drop any drag-time cursor; the next pointermove will re-pick "grab" if the pointer
        // happens to be resting over a city.
        r.canvas.style.cursor = "";
        try { r.canvas.releasePointerCapture(e.pointerId); } catch { }
    };
    r.canvas.addEventListener("pointerup", endDrag);
    r.canvas.addEventListener("pointercancel", endDrag);
    r.canvas.addEventListener("pointerleave", () => {
        if (!mode) r.canvas.style.cursor = "";
    });
    // Right-click drag drives the tilt camera; without this preventDefault the browser would
    // pop its context menu on mousedown and steal the gesture. The pointerdown handler above
    // has already started the drag by the time this fires, so suppressing the menu is enough
    // to let the rest of the gesture flow through pointermove/pointerup as normal.
    r.canvas.addEventListener("contextmenu", (e) => e.preventDefault());
    r.canvas.addEventListener("wheel", (e) => {
        e.preventDefault();
        const next = Math.max(0.25, Math.min(4.0, r.zoom * Math.exp(-e.deltaY * 0.001)));
        if (next === r.zoom) return;
        r.zoom = next;
        r.worker.postMessage({ type: "camera", zoom: next });
    }, { passive: false });
}

// Hit-test cities under the pointer. Returns the matching city id or null. Works in
// device-pixel "world" space (the same coordinates rebuildStatic uses to draw the dots),
// inverting the live camera transform so the test stays accurate under any combination of
// pan, zoom, pitch, and yaw.
//
// The forward transform of a world point (x, y, 0) into device-pixel screen space is:
//   sx = cx + zoom * cos(yaw)   * (x - cx + panX)
//   sy = cy + zoom * cos(pitch) * (y - cy + panY)
//             + zoom * sin(pitch) * sin(yaw) * (x - cx + panX)
// Note the sin(pitch)*sin(yaw) cross term: a separable inverse ("undo cos(yaw) on x, undo
// cos(pitch) on y") gets x right but offsets y by an amount that easily exceeds the dot's
// hit radius once both axes are tilted. The proper inverse below recovers it exactly.
function hitTestCity(r, e) {
    if (!r.proj || !r.cities || r.cities.length === 0) return null;
    const rect = r.canvas.getBoundingClientRect();
    const devX = (e.clientX - rect.left) * r.dpr;
    const devY = (e.clientY - rect.top) * r.dpr;
    const cx = r.proj.width / 2;
    const cy = r.proj.height / 2;
    const cosP = Math.max(0.0001, Math.cos(r.pitchRad || 0));
    const cosY = Math.max(0.0001, Math.cos(r.yawRad || 0));
    const sinP = Math.sin(r.pitchRad || 0);
    const sinY = Math.sin(r.yawRad || 0);
    const tanY = sinY / cosY;
    const zoom = Math.max(0.0001, r.zoom);
    const A = devX - cx;
    const B = devY - cy;
    const worldX = cx - (r.panX || 0) + A / (zoom * cosY);
    const worldY = cy - (r.panY || 0) + (B - sinP * tanY * A) / (zoom * cosP);
    const radius = CityHitPx(r);
    let best = null;
    let bestD2 = radius * radius;
    for (const c of r.cities) {
        const px = r.proj.offsetX + c.x * r.proj.scale;
        const py = r.proj.offsetY + c.y * r.proj.scale;
        const d2 = (px - worldX) * (px - worldX) + (py - worldY) * (py - worldY);
        if (d2 <= bestD2) {
            bestD2 = d2;
            best = c.id;
        }
    }
    return best;
}

// Hit radius in device-pixel "world" units (the same space the static map dots are drawn
// in). The visible city circle has world radius `cityR = max(3, 4*dpr)` (mirrored from
// fleet-worker.js's rebuildStatic), and its on-screen area scales with zoom². To keep small
// dots grabbable when zoomed out and to keep the hit zone modest when zoomed in, we set the
// hit-area-to-circle-area ratio as:
//
//   ratio = max(3 / zoom², 1.5)
//
// At zoom=1 the hit zone is exactly 3× the circle area (radius factor √3). When zoomed out
// (zoom<1) the ratio grows like 1/zoom², which is the same factor by which the circle's
// on-screen area shrinks -- so the screen-space hit zone stays the same physical size, making
// shrinking dots progressively easier to pick. When zoomed in (zoom>1) the ratio collapses
// past 1.5 around zoom≈√2 and is clamped there, so the hit zone never gets uncomfortably
// tight on a clearly-visible dot.
function CityHitPx(r) {
    const dpr = r.dpr || 1;
    const cityR = Math.max(3, 4 * dpr);
    const zoom = Math.max(0.0001, r.zoom || 1);
    const ratio = Math.max(1.5, 3 / (zoom * zoom));
    return cityR * Math.sqrt(ratio);
}

function pushViewport(r) {
    const dpr = Math.max(1, window.devicePixelRatio || 1);
    r.dpr = dpr;
    const cssW = r.canvas.clientWidth || 800;
    const cssH = r.canvas.clientHeight || 600;
    const w = Math.max(1, Math.floor(cssW * dpr));
    const h = Math.max(1, Math.floor(cssH * dpr));
    r.worker.postMessage({ type: "viewport", width: w, height: h, dpr });
}

function onWorkerMessage(r, m) {
    if (m && m.type === "viewport-proj") {
        // Mirror the projection so input handlers can map pointer events to graph coords
        // without poking the worker.
        r.proj = { scale: m.scale, offsetX: m.offsetX, offsetY: m.offsetY,
                   width: m.width, height: m.height };
        if (r.dotnetRef) {
            // Fire-and-forget: a stale projection only causes one frame of slightly wrong
            // pre-projection, which is invisible at the dot radius we draw.
            r.dotnetRef.invokeMethodAsync("OnViewportChanged",
                m.scale, m.offsetX, m.offsetY, m.width, m.height);
        }
    }
}

function setLayout(canvasId, layoutJson, dotnetRef) {
    const r = ensure(canvasId, dotnetRef);
    if (!r) return;
    // Mirror the cities so we can hit-test on pointerdown without bouncing through the
    // worker. The worker mutates its own copy when the user drags a city; we mutate ours in
    // lockstep, so the two never diverge.
    try {
        const layout = JSON.parse(layoutJson);
        r.cities = (layout.cities || []).map((c) => ({ id: c.id, x: c.x, y: c.y }));
    } catch {
        r.cities = null;
    }
    r.worker.postMessage({ type: "layout", layoutJson });
}

// `buffer` is a Uint8Array marshalled directly from a Blazor byte[]. We transfer the
// underlying ArrayBuffer to the worker so there is no copy across the postMessage boundary.
// Blazor allocates a fresh array per call, so transferring (which detaches the main-thread
// view) is safe.
function update(canvasId, buffer) {
    const r = renderers.get(canvasId);
    if (!r) return;
    const ab = buffer.buffer;
    r.worker.postMessage({ type: "update", buffer: ab }, [ab]);
}

function dispose(canvasId) {
    const r = renderers.get(canvasId);
    if (!r) return;
    r.observer?.disconnect();
    try { r.worker.postMessage({ type: "dispose" }); } catch { }
    r.worker.terminate();
    renderers.delete(canvasId);
}

// Blazor-driven camera updates. Partial-field: only the keys present in `cam` are applied,
// mirroring the worker's handleCamera semantics so independent input surfaces don't have to
// know each other's state. Tilt is normally driven directly by the canvas pointer handler;
// this entry point remains for any future programmatic resets.
function setCamera(canvasId, cam) {
    const r = renderers.get(canvasId);
    if (!r || !cam) return;
    if (typeof cam.pitchRad === "number") r.pitchRad = cam.pitchRad;
    if (typeof cam.yawRad === "number") r.yawRad = cam.yawRad;
    if (typeof cam.zoom === "number") r.zoom = Math.max(0.25, Math.min(4.0, cam.zoom));
    if (typeof cam.panX === "number") r.panX = cam.panX;
    if (typeof cam.panY === "number") r.panY = cam.panY;
    r.worker.postMessage({ type: "camera", ...cam });
}

window.fleetCanvas = { setLayout, update, dispose, setCamera };
