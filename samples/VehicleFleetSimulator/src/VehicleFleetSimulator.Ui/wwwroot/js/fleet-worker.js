// Worker-side fleet renderer.
//
// Runs on a dedicated Web Worker thread, completely off the Blazor main thread. Owns:
//   * `canvas`        -- the visible OffscreenCanvas, transferred from the main thread.
//   * `gl`            -- a WebGL2 context on that canvas, used for both the static map quad
//                        (textured) and the per-vehicle dots (instanced point sprites).
//   * `staticCanvas`  -- a private OffscreenCanvas with a 2D context. We draw the map (edges,
//                        nodes, labels) into it once per layout/resize and upload it as a
//                        single texture to GL. Per-frame the map is just a fullscreen quad
//                        sampling that texture, so the entire fixed scenery is one draw call.
//   * `vehicles`      -- Map<idHex, {x,y,tx,ty,status}> per vehicle: (x, y) is the rendered
//                        position, (tx, ty) is the latest target reported by C#. Each frame
//                        the rendered position eases toward the target via exponential
//                        smoothing -- no two-sample buffer, no segment boundaries, no clamps.
//
// Vehicles render via instanced rendering: a 4-vertex unit quad shared between all instances,
// plus a per-instance buffer of (x, y, status). One `drawArraysInstanced` draws the whole
// fleet regardless of size -- this is the move from O(N) Canvas2D fill calls to O(1).
//
// We previously buffered the last two samples and linearly interpolated at `now - bufferDelay`.
// That is mathematically equivalent to chasing the moving target with a fixed lag, but only
// when `bufferDelay` exactly matches the inter-packet gap. In practice the C# render loop and
// the Orleans grain timers are independent 200 ms clocks whose phases drift, so observed gaps
// wobble by tens of ms around the EMA. Whenever `bufferDelay != gap` the renderTime falls
// either side of the segment, gets clamped to t=0 or t=1, and the vehicle stutters at the
// boundary -- 1-3 frames of stillness, five times per second, perceived as stop/start in
// lockstep across the whole fleet (they all share the same scheduler). Exponential smoothing
// has no boundary, so it has nothing to clamp at; for a vehicle moving at constant velocity
// it produces the same ~one-tick rendered lag as the old buffered scheme but is immune to
// packet-arrival jitter.

let canvas = null;
let gl = null;
let staticCanvas = null;
let staticCtx = null;
let staticTex = null;
// Labels live on a separate transparent overlay so we can draw them *after* the vehicle pass
// and keep them readable when dots cluster on top of cities. Same shader as the base map.
let labelsCanvas = null;
let labelsCtx = null;
let labelsTex = null;

let layout = null;
let cityById = null;
let proj = null;            // {scale, offsetX, offsetY} -- in canvas-pixel space
let dpr = 1;
let width = 0;
let height = 0;
let vehicleR = 0;

const vehicles = new Map();
let rafId = 0;

// Buffer delay used by the rAF interpolation loop. We render `bufferDelayMs` in the past
// so `t` always lies in [0, 1] between two received samples. It must match the actual rate
// at which C# pushes packets, not a hardcoded constant -- the user can change that rate at
// runtime via the Sync slider (50 ms .. 2 000 ms). When the buffer delay drifts above the
// real interval, every segment starts with `t < 0` (clamped to 0) and the vehicle visibly
// stalls at its previous position before snapping forward; when it drifts below, t never
// reaches 1 and packets effectively get skipped past.
//
// Time constant for the per-vehicle exponential smoothing, in seconds. About 63% of any gap
// between rendered position and target is closed every TAU seconds; 95% is closed in 3*TAU.
// 0.2 s matches the simulator tick cadence so a constantly-moving vehicle's rendered position
// trails its latest reported sample by ~one tick -- visually identical to the old
// buffer-delay scheme but with no boundary to stutter at.
const SMOOTHING_TAU_SEC = 0.2;
let lastFrameTime = 0;

// Programs/buffers/uniforms.
let mapProg = null, mapVAO = null, mapVBO = null, uMapTex = null, uMapMvp = null, uMapSize = null, uMapOrigin = null;
let dotProg = null, dotVAO = null, quadVBO = null, instVBO = null;
let uResolution = null, uRadius = null, uColors = null, uDotMvp = null;

// Padding factor for the static map texture. Each side of the offscreen bitmap is padded by
// STATIC_PAD_FACTOR * (visible canvas dimension), so the total padded canvas area is
// (1 + 2*M)² × visible. With M = 1.5 the padded extent is 4× each visible dimension, which
// exactly matches the camera's minimum zoom (0.25) -- at max zoom-out with pan=0 the entire
// padded texture fills the view and nothing is clipped. The padded region also lets users drag
// cities well outside the original layout bounds without their edges/labels disappearing into
// the offscreen-canvas boundary. The actual factor used per rebuild is clamped against the
// GPU's MAX_TEXTURE_SIZE so this scheme degrades gracefully on tile-bound mobile GPUs.
const STATIC_PAD_FACTOR = 1.5;
// World-space rectangle the static map quad currently spans. Updated by rebuildStatic so the
// draw passes can feed the matching origin + size into the map shader; defaults preserve the
// original (0,0) → (width,height) behaviour for the brief window before the first rebuild.
let mapOriginX = 0, mapOriginY = 0, mapSizeX = 0, mapSizeY = 0;

// 2.5D camera. World coordinates are exactly the existing canvas-pixel coordinates (so the
// C# pre-projection in BuildVehiclePacket needs no changes). The static map / labels quads
// span world space (0,0) → (width,height) on the z=0 plane; vehicle centres are also at z=0.
// MVP composes:
//   1. Centre the world on the origin (translate by -width/2, -height/2)
//   2. Pan in world units (translate by panX, panY)
//   3. Zoom (uniform scale)
//   4. Yaw around the Y axis (rotateY by yaw) -- horizontal tilt
//   5. Tilt around the X axis (rotateX by pitch) -- vertical tilt
//   6. Orthographic projection sized to the canvas, with the y axis flipped so canvas-down
//      maps to clip-up.
// At pitch=0, yaw=0, zoom=1, pan=(0,0) the resulting MVP is identity (in NDC terms) and the
// scene looks exactly like the old 2D renderer.
const camera = { panX: 0, panY: 0, zoom: 1, pitchRad: 0, yawRad: 0 };
const mvpMat = new Float32Array(16);
const tmpMat = new Float32Array(16);

// Per-instance scratch buffer for interpolated positions. Float32Array of x, y, status, fuel.
const STRIDE_FLOATS = 4;
let instCapacity = 0;
let instData = null;

// Status -> RGBA. Index 0 is the "unknown / unspecified" fallback, which we colour like
// driving so a status enum drift doesn't render the fleet invisible.
//   1 idle           = #94a3b8
//   2 driving        = #3b82f6
//   3 refuelling     = #f59e0b
//   4 routeCompleted = #22c55e
const COLORS = new Float32Array([
    0.231, 0.510, 0.965, 1.0,
    0.580, 0.639, 0.722, 1.0,
    0.231, 0.510, 0.965, 1.0,
    0.961, 0.620, 0.043, 1.0,
    0.133, 0.773, 0.369, 1.0,
]);

self.onmessage = (e) => {
    const m = e.data;
    if (!m) return;
    switch (m.type) {
        case "init":     handleInit(m); break;
        case "layout":   handleLayout(m); break;
        case "viewport": handleViewport(m); break;
        case "update":   handleUpdate(m); break;
        case "camera":   handleCamera(m); break;
        case "cityMove": handleCityMove(m); break;
        case "dispose":  handleDispose(); break;
    }
};

function handleInit(m) {
    canvas = m.canvas;
    dpr = m.dpr || 1;
    gl = canvas.getContext("webgl2", { antialias: false, premultipliedAlpha: true, alpha: false });
    if (!gl) {
        self.postMessage({ type: "error", message: "WebGL2 unavailable in worker" });
        return;
    }
    initGL();
}

function handleLayout(m) {
    layout = JSON.parse(m.layoutJson);
    cityById = new Map(layout.cities.map((c) => [c.id, c]));
    rebuildStatic();
    ensureRaf();
}

function handleViewport(m) {
    if (!gl) return;
    width = m.width;
    height = m.height;
    dpr = m.dpr || 1;
    if (canvas.width !== width) canvas.width = width;
    if (canvas.height !== height) canvas.height = height;
    gl.viewport(0, 0, width, height);
    rebuildStatic();
    ensureRaf();
}

function handleUpdate(m) {
    if (!proj) return;
    const view = new DataView(m.buffer);
    const count = view.getUint32(0, true);

    // Set each vehicle's target to the latest reported position. The rendered (x, y) is
    // tweened toward (tx, ty) on every animation frame, so packet-arrival timing simply
    // doesn't enter the equation here -- jittery gaps, duplicate same-position packets,
    // mid-burst reads on the C# side: all benign. New vehicles snap to their starting
    // position so they appear instantly; existing vehicles keep their current rendered
    // position and just retarget.
    const seen = new Set();
    for (let i = 0; i < count; i++) {
        const o = 4 + i * 22;
        // Compose a stable string key from the two halves of the Guid. Hex strings get
        // interned by V8, so subsequent updates of the same vehicle hit the same key object.
        const lo = view.getBigUint64(o, true).toString(16).padStart(16, "0");
        const hi = view.getBigUint64(o + 8, true).toString(16).padStart(16, "0");
        const key = lo + hi;
        seen.add(key);

        const x = view.getInt16(o + 16, true);
        const y = view.getInt16(o + 18, true);
        const status = view.getUint8(o + 20);
        // Fuel arrives quantised to 0..255; rescale to a 0..1 float for the vertex shader.
        const fuel = view.getUint8(o + 21) / 255;

        let v = vehicles.get(key);
        if (!v) {
            v = { x, y, tx: x, ty: y, status, fuel };
            vehicles.set(key, v);
        } else {
            v.tx = x; v.ty = y;
            v.status = status;
            v.fuel = fuel;
        }
    }
    if (vehicles.size !== seen.size) {
        for (const k of vehicles.keys()) if (!seen.has(k)) vehicles.delete(k);
    }
}

function handleDispose() {
    if (rafId) {
        cancelAnimationFrame(rafId);
        rafId = 0;
    }
    vehicles.clear();
}

function handleCamera(m) {
    // Partial updates: only fields the main thread sends in this message are applied. This
    // lets pan, zoom, and tilt come from independent input handlers without each having to
    // know the others' current values.
    if (typeof m.panX === "number") camera.panX = m.panX;
    if (typeof m.panY === "number") camera.panY = m.panY;
    if (typeof m.zoom === "number" && m.zoom > 0) camera.zoom = m.zoom;
    if (typeof m.pitchRad === "number") camera.pitchRad = m.pitchRad;
    if (typeof m.yawRad === "number") camera.yawRad = m.yawRad;
}

// User-driven city repositioning. The main-thread shim hit-tests on pointerdown and forwards
// graph-space (layout) coordinates here. We mutate the city object in-place; cityById holds
// references into the same layout.cities array, so both views stay coherent. We deliberately
// don't recompute layout.bounds: shifting the projection mid-drag would yank everything else
// around under the cursor, which is disorienting. Cities can stray slightly past the original
// bounds without visual consequence.
function handleCityMove(m) {
    if (!cityById) return;
    const c = cityById.get(m.id);
    if (!c) return;
    c.x = m.x;
    c.y = m.y;
    rebuildStatic();
}

function ensureRaf() {
    if (rafId) return;
    const tick = () => {
        rafId = requestAnimationFrame(tick);
        draw();
    };
    rafId = requestAnimationFrame(tick);
}

function initGL() {
    gl.disable(gl.DEPTH_TEST);
    gl.disable(gl.CULL_FACE);
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

    // Static-map program: textured world-space quad anchored at u_mapOrigin with extent
    // u_mapSize on the z=0 plane. Originally the quad always started at world (0,0); the origin
    // uniform was added so the padded static texture (see STATIC_PAD_FACTOR) can sit centred
    // around the visible canvas at world (-padX, -padY).
    mapProg = link(compile(gl.VERTEX_SHADER, MAP_VS), compile(gl.FRAGMENT_SHADER, MAP_FS));
    uMapTex = gl.getUniformLocation(mapProg, "u_tex");
    uMapMvp = gl.getUniformLocation(mapProg, "u_mvp");
    uMapSize = gl.getUniformLocation(mapProg, "u_mapSize");
    uMapOrigin = gl.getUniformLocation(mapProg, "u_mapOrigin");
    mapVAO = gl.createVertexArray();
    mapVBO = gl.createBuffer();
    gl.bindVertexArray(mapVAO);
    gl.bindBuffer(gl.ARRAY_BUFFER, mapVBO);
    // (norm x, norm y) in [0..1] for both position and uv. The vertex shader scales by
    // u_mapSize to get world coordinates; the fragment samples the texture at the same uv.
    // Triangle-strip order: TL, TR, BL, BR. Canvas y=0 is the visual top, which corresponds
    // to texture v=0 (UNPACK_FLIP_Y_WEBGL stays at its default of false).
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([
        0, 0,
        1, 0,
        0, 1,
        1, 1,
    ]), gl.STATIC_DRAW);
    const aPos = gl.getAttribLocation(mapProg, "a_pos");
    gl.enableVertexAttribArray(aPos);
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 8, 0);
    gl.bindVertexArray(null);

    staticTex = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, staticTex);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);

    labelsTex = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, labelsTex);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);

    // Dot program: per-instance vec2 center + float status, shared 4-vertex unit quad.
    dotProg = link(compile(gl.VERTEX_SHADER, DOT_VS), compile(gl.FRAGMENT_SHADER, DOT_FS));
    uResolution = gl.getUniformLocation(dotProg, "u_resolution");
    uRadius = gl.getUniformLocation(dotProg, "u_radius");
    uColors = gl.getUniformLocation(dotProg, "u_colors");
    uDotMvp = gl.getUniformLocation(dotProg, "u_mvp");

    dotVAO = gl.createVertexArray();
    quadVBO = gl.createBuffer();
    instVBO = gl.createBuffer();
    gl.bindVertexArray(dotVAO);

    gl.bindBuffer(gl.ARRAY_BUFFER, quadVBO);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([
        -1, -1,
         1, -1,
        -1,  1,
         1,  1,
    ]), gl.STATIC_DRAW);
    const aCorner = gl.getAttribLocation(dotProg, "a_corner");
    gl.enableVertexAttribArray(aCorner);
    gl.vertexAttribPointer(aCorner, 2, gl.FLOAT, false, 8, 0);

    gl.bindBuffer(gl.ARRAY_BUFFER, instVBO);
    const aCenter = gl.getAttribLocation(dotProg, "a_center");
    gl.enableVertexAttribArray(aCenter);
    gl.vertexAttribPointer(aCenter, 2, gl.FLOAT, false, 16, 0);
    gl.vertexAttribDivisor(aCenter, 1);
    const aStatus = gl.getAttribLocation(dotProg, "a_status");
    gl.enableVertexAttribArray(aStatus);
    gl.vertexAttribPointer(aStatus, 1, gl.FLOAT, false, 16, 8);
    gl.vertexAttribDivisor(aStatus, 1);
    const aFuel = gl.getAttribLocation(dotProg, "a_fuel");
    gl.enableVertexAttribArray(aFuel);
    gl.vertexAttribPointer(aFuel, 1, gl.FLOAT, false, 16, 12);
    gl.vertexAttribDivisor(aFuel, 1);

    gl.bindVertexArray(null);
}

function compile(type, src) {
    const s = gl.createShader(type);
    gl.shaderSource(s, src);
    gl.compileShader(s);
    if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
        const log = gl.getShaderInfoLog(s);
        throw new Error("shader compile failed: " + log);
    }
    return s;
}

function link(vs, fs) {
    const p = gl.createProgram();
    gl.attachShader(p, vs);
    gl.attachShader(p, fs);
    gl.linkProgram(p);
    if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
        const log = gl.getProgramInfoLog(p);
        throw new Error("program link failed: " + log);
    }
    return p;
}

function rebuildStatic() {
    if (!gl || !layout || !width || !height) return;

    // Pick the effective padding factor. Honour STATIC_PAD_FACTOR unless it would push either
    // padded dimension past the GPU's max texture size, in which case we shrink M to fit. The
    // visible projection (proj.scale/offsetX/Y, used by hit-test + C# vehicle pre-projection)
    // is unaffected by the padding -- only the offscreen bitmap and the world-space rect of
    // the map quad grow.
    const maxTex = gl.getParameter(gl.MAX_TEXTURE_SIZE) || 4096;
    const maxByGpu = (maxTex / Math.max(width, height) - 1) / 2;
    const M = Math.max(0, Math.min(STATIC_PAD_FACTOR, maxByGpu));
    const padX = Math.round(M * width);
    const padY = Math.round(M * height);
    const paddedW = width + padX * 2;
    const paddedH = height + padY * 2;

    if (!staticCanvas || staticCanvas.width !== paddedW || staticCanvas.height !== paddedH) {
        staticCanvas = new OffscreenCanvas(paddedW, paddedH);
        staticCtx = staticCanvas.getContext("2d");
    }
    const ctx = staticCtx;
    const padPx = Math.round(28 * dpr);
    const innerW = width - padPx * 2;
    const innerH = height - padPx * 2;
    const b = layout.bounds;
    const dx = Math.max(1e-6, b.maxX - b.minX);
    const dy = Math.max(1e-6, b.maxY - b.minY);
    const scale = Math.min(innerW / dx, innerH / dy);
    // Visible-space offsets: identical to the original fit-to-canvas computation. These are
    // what get reported to the main thread + C# so the rest of the system keeps treating the
    // visible canvas as the source of truth.
    const offsetX = padPx + (innerW - dx * scale) / 2 - b.minX * scale;
    const offsetY = padPx + (innerH - dy * scale) / 2 - b.minY * scale;
    proj = { scale, offsetX, offsetY };
    // Texture-space offsets: shifted by (padX, padY) so the same world-space city position
    // ends up at the matching pixel inside the padded bitmap. The map quad is then drawn
    // spanning world (-padX, -padY) → (width + padX, height + padY); the texture's [0..1] uv
    // covers that whole rectangle, so a city drawn at texOffset + c.x*scale lands at the
    // correct world position once the camera transform is applied.
    const texOffsetX = offsetX + padX;
    const texOffsetY = offsetY + padY;
    mapOriginX = -padX;
    mapOriginY = -padY;
    mapSizeX = paddedW;
    mapSizeY = paddedH;

    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.fillStyle = "#000";
    ctx.fillRect(0, 0, paddedW, paddedH);

    // Edges. Each undirected edge is drawn as TWO quadratic-bezier arcs that bow in opposite
    // directions, one per traffic direction (A->B uses the +perp arc, B->A uses the -perp arc).
    // Vehicle positions in C# follow the matching parabolic curve, so dots stay glued to the
    // line. Amplitude of 8% of edge length keeps adjacent arcs clearly separated without making
    // the map look like spaghetti for short edges.
    ctx.lineWidth = Math.max(1, dpr);
    ctx.strokeStyle = "rgba(160, 174, 192, 0.35)";
    ctx.beginPath();
    for (const e of layout.edges) {
        const a = cityById.get(e.a);
        const c = cityById.get(e.b);
        if (!a || !c) continue;
        const ax = texOffsetX + a.x * scale, ay = texOffsetY + a.y * scale;
        const bx = texOffsetX + c.x * scale, by = texOffsetY + c.y * scale;
        const ex = bx - ax, ey = by - ay;
        const len = Math.hypot(ex, ey);
        if (len < 1e-3) continue;
        const amp = 0.08 * len;
        const px = -ey / len * amp;
        const py = ex / len * amp;
        const mx = (ax + bx) * 0.5, my = (ay + by) * 0.5;
        ctx.moveTo(ax, ay);
        ctx.quadraticCurveTo(mx + 2 * px, my + 2 * py, bx, by);
        ctx.moveTo(bx, by);
        ctx.quadraticCurveTo(mx - 2 * px, my - 2 * py, ax, ay);
    }
    ctx.stroke();

    // City nodes.
    const cityR = Math.max(3, 4 * dpr);
    vehicleR = Math.max(2, 3 * dpr);
    ctx.fillStyle = "#e2e8f0";
    ctx.strokeStyle = "rgba(0,0,0,0.4)";
    ctx.lineWidth = Math.max(1, dpr);
    for (const c of layout.cities) {
        const x = texOffsetX + c.x * scale;
        const y = texOffsetY + c.y * scale;
        ctx.beginPath();
        ctx.arc(x, y, cityR, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
    }

    // Upload base map (edges + nodes) to its texture. Labels are baked separately below.
    gl.bindTexture(gl.TEXTURE_2D, staticTex);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, staticCanvas);

    // City labels go on a separate fully-transparent overlay, drawn after vehicles so dots
    // never occlude the names. Keeping it on its own canvas means we still pay for the
    // text-shaping work only on layout/resize, not per frame.
    if (!labelsCanvas || labelsCanvas.width !== paddedW || labelsCanvas.height !== paddedH) {
        labelsCanvas = new OffscreenCanvas(paddedW, paddedH);
        labelsCtx = labelsCanvas.getContext("2d");
    }
    const lctx = labelsCtx;
    lctx.setTransform(1, 0, 0, 1, 0, 0);
    lctx.clearRect(0, 0, paddedW, paddedH);
    const labelFontPx = Math.max(11, 12 * dpr);
    lctx.font = `${labelFontPx}px system-ui, -apple-system, "Segoe UI", sans-serif`;
    lctx.textBaseline = "middle";
    const labelGap = cityR + Math.max(4, 4 * dpr);
    const labelOffsetY = -Math.max(2, 2 * dpr);
    const edgeGuard = Math.max(4, 4 * dpr);
    lctx.shadowColor = "rgba(0,0,0,0.85)";
    lctx.shadowBlur = Math.max(2, 3 * dpr);
    lctx.fillStyle = "#f1f5f9";
    for (const c of layout.cities) {
        const x = texOffsetX + c.x * scale;
        const y = texOffsetY + c.y * scale;
        const label = c.name || c.id;
        const tw = lctx.measureText(label).width;
        // Flip-to-right alignment when the label would overflow the *padded* bitmap. We don't
        // care about overflowing the visible canvas: with the camera pannable, that boundary
        // is meaningless mid-drag and we'd otherwise see labels jump from left- to right-side
        // for no apparent reason as a city is dragged across the visible edge.
        if ((x + labelGap + tw + edgeGuard) > paddedW) {
            lctx.textAlign = "right";
            lctx.fillText(label, x - labelGap, y + labelOffsetY);
        } else {
            lctx.textAlign = "left";
            lctx.fillText(label, x + labelGap, y + labelOffsetY);
        }
    }
    lctx.shadowBlur = 0;

    gl.bindTexture(gl.TEXTURE_2D, labelsTex);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, labelsCanvas);

    // Tell the main thread (and through it, Blazor C#) the new projection. C# uses these
    // numbers to pre-project layout coordinates into screen space inside BuildVehiclePacket.
    self.postMessage({ type: "viewport-proj", scale, offsetX, offsetY, width, height });
}

function draw() {
    if (!gl || !proj || !width || !height) return;

    gl.viewport(0, 0, width, height);
    composeMvp(mvpMat, width, height, camera);

    // 1. Map. World-space quad (mapOriginX, mapOriginY) -> + (mapSizeX, mapSizeY), transformed
    //    by MVP. The padded static texture spans world (-padX, -padY) → (width+padX, height+padY)
    //    so cities/edges dragged outside the visible canvas still get rendered. At identity
    //    camera the texture maps onto the visible region exactly as before; the padding shows
    //    only when zoomed out or panned past an edge.
    gl.useProgram(mapProg);
    gl.bindVertexArray(mapVAO);
    gl.uniformMatrix4fv(uMapMvp, false, mvpMat);
    gl.uniform2f(uMapSize, mapSizeX || width, mapSizeY || height);
    gl.uniform2f(uMapOrigin, mapOriginX, mapOriginY);
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, staticTex);
    gl.uniform1i(uMapTex, 0);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);

    // 2. Vehicles. Skip the entire pipeline when the fleet is empty so an idle simulator
    //    contributes ~zero GPU work.
    if (vehicles.size === 0) {
        drawLabelsOverlay();
        return;
    }

    if (instCapacity < vehicles.size) {
        instCapacity = Math.max(64, vehicles.size * 2);
        instData = new Float32Array(instCapacity * STRIDE_FLOATS);
    }

    // Per-frame exponential ease-out: x += (tx - x) * (1 - exp(-dt / TAU)). The dt clamp
    // (50 ms ceiling) protects against the long pauses that browsers impose on background
    // tabs -- when the tab returns to the foreground we don't want every vehicle to snap to
    // its target in a single frame. The 1 ms floor avoids alpha = 0 on rare 0-dt frames.
    const now = performance.now();
    let dt = lastFrameTime > 0 ? (now - lastFrameTime) / 1000 : 1 / 60;
    lastFrameTime = now;
    if (dt < 0.001) dt = 0.001;
    else if (dt > 0.05) dt = 0.05;
    const alpha = 1 - Math.exp(-dt / SMOOTHING_TAU_SEC);

    let n = 0;
    for (const v of vehicles.values()) {
        v.x += (v.tx - v.x) * alpha;
        v.y += (v.ty - v.y) * alpha;
        const off = n * STRIDE_FLOATS;
        instData[off] = v.x;
        instData[off + 1] = v.y;
        instData[off + 2] = v.status;
        instData[off + 3] = v.fuel;
        n++;
    }

    gl.bindBuffer(gl.ARRAY_BUFFER, instVBO);
    // bufferData with a typed-array view uploads only the live portion. Using DYNAMIC_DRAW
    // keeps the driver from over-allocating immutable storage.
    gl.bufferData(gl.ARRAY_BUFFER, instData.subarray(0, n * STRIDE_FLOATS), gl.DYNAMIC_DRAW);

    gl.useProgram(dotProg);
    gl.bindVertexArray(dotVAO);
    gl.uniformMatrix4fv(uDotMvp, false, mvpMat);
    gl.uniform2f(uResolution, width, height);
    gl.uniform1f(uRadius, vehicleR);
    gl.uniform4fv(uColors, COLORS);
    gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, n);

    drawLabelsOverlay();
}

function drawLabelsOverlay() {
    // Labels overlay -- drawn last so vehicle dots never sit on top of city names. Uses
    // mapProg with the same MVP and world-quad geometry as the base map; the texture is
    // mostly transparent so standard alpha blending leaves the underlying scene intact.
    gl.useProgram(mapProg);
    gl.bindVertexArray(mapVAO);
    gl.uniformMatrix4fv(uMapMvp, false, mvpMat);
    gl.uniform2f(uMapSize, mapSizeX || width, mapSizeY || height);
    gl.uniform2f(uMapOrigin, mapOriginX, mapOriginY);
    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, labelsTex);
    gl.uniform1i(uMapTex, 0);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

// --- Shaders -----------------------------------------------------------------------------

const MAP_VS = `#version 300 es
in vec2 a_pos;
uniform mat4 u_mvp;
uniform vec2 u_mapSize;
uniform vec2 u_mapOrigin;
out vec2 v_uv;
void main() {
    // a_pos is a normalised [0..1] quad corner; u_mapOrigin + a_pos * u_mapSize gives the
    // canvas-pixel world coords. The same a_pos drives the UV directly so the static texture
    // (indexed top-left -> bottom-right) maps unchanged onto whatever world rectangle the
    // origin/size pair describes. With origin = (0,0) and size = (width, height) this is the
    // original fullscreen-quad behaviour; with the padded scheme the quad spans
    // (-padX, -padY) → (width + padX, height + padY).
    v_uv = a_pos;
    vec3 world = vec3(u_mapOrigin + a_pos * u_mapSize, 0.0);
    gl_Position = u_mvp * vec4(world, 1.0);
}`;

const MAP_FS = `#version 300 es
precision mediump float;
in vec2 v_uv;
uniform sampler2D u_tex;
out vec4 outColor;
void main() {
    outColor = texture(u_tex, v_uv);
}`;

// Per-instance: a_center is in canvas-pixel world space (z = 0 plane). The vertex shader
// projects the centre via MVP, then offsets in clip space by a screen-pixel-sized corner so
// the dot stays a constant size regardless of zoom or tilt.
const DOT_VS = `#version 300 es
in vec2 a_corner;
in vec2 a_center;
in float a_status;
in float a_fuel;
uniform mat4 u_mvp;
uniform vec2 u_resolution;
uniform float u_radius;
out vec2 v_quad;
out float v_status;
out float v_fuel;
void main() {
    vec4 clipCenter = u_mvp * vec4(a_center, 0.0, 1.0);
    // Convert the unit-quad corner offset into clip space: a pixel radius is
    // 2 * radius / resolution in NDC. The quad VBO has y=-1 at the visual top, but in clip
    // space y=+1 is up, so flip the corner's y component on the way in.
    vec2 cornerNDC = vec2(a_corner.x, -a_corner.y) * u_radius * 2.0 / u_resolution;
    // Multiply by clipCenter.w so the offset survives the perspective divide unchanged.
    // For our orthographic projection w == 1 so this is a no-op, but it keeps the shader
    // correct if we ever swap to perspective.
    gl_Position = vec4(clipCenter.xy + cornerNDC * clipCenter.w, clipCenter.zw);
    v_quad = a_corner;
    v_status = a_status;
    v_fuel = a_fuel;
}`;

// The fragment is a soft-edged disk. `discard` outside the unit circle keeps us free of
// quad-corner artefacts; `smoothstep` gives a 1-pixel-ish anti-aliased border.
//
// Driving vehicles get a fuel-driven gradient (full = green, empty = red) instead of the
// flat status colour, so the user can tell at a glance which dots are about to head for a
// pump. Other statuses (idle, refuelling, route-completed) keep their solid colour because
// the gradient would be meaningless or actively misleading for them (a refuelling vehicle
// near zero fuel would otherwise read as "in trouble" rather than "being topped up").
const DOT_FS = `#version 300 es
precision mediump float;
in vec2 v_quad;
in float v_status;
in float v_fuel;
uniform vec4 u_colors[5];
out vec4 outColor;
void main() {
    float d = length(v_quad);
    if (d > 1.0) discard;
    int idx = int(v_status);
    if (idx < 0 || idx > 4) idx = 0;
    vec4 c = u_colors[idx];
    if (idx == 2) {
        // Driving: 3-stop gradient red -> yellow -> green by fuel fraction. A linear two-stop
        // RGB lerp from red to green crosses through olive/brown around 0.5, which reads as
        // "dirty orange" -- the wrong signal -- so vehicles spend most of their lives looking
        // alarmed even when they're at half a tank. Routing through yellow keeps the gauge
        // perceptually monotonic: fuller is brighter green, lower is genuine orange/red.
        //
        // Linear fuel-fraction gradient. The simulator now models a realistic 500 L HGV tank
        // burning ~36 L per 100 km segment, so a vehicle spends most of its life above the
        // halfway mark and only dips toward red in the segment or two before refuelling --
        // a faithful linear gauge tells that story without needing a perceptual curve. The
        // 3-stop mix (red -> saturated yellow -> green) avoids the muddy olive midtone you
        // get from a direct red->green RGB lerp.
        vec3 empty = vec3(0.937, 0.267, 0.267); // #ef4444
        vec3 mid   = vec3(1.000, 0.835, 0.200); // #ffd533
        vec3 full  = vec3(0.133, 0.773, 0.369); // #22c55e
        float f = clamp(v_fuel, 0.0, 1.0);
        vec3 rgb = f < 0.5
            ? mix(empty, mid, f * 2.0)
            : mix(mid, full, (f - 0.5) * 2.0);
        c = vec4(rgb, 1.0);
    }
    float a = smoothstep(1.0, 0.85, d);
    outColor = vec4(c.rgb, c.a * a);
}`;

// --- Camera maths --------------------------------------------------------------------
//
// All matrices are column-major Float32Array(16), the layout glUniformMatrix4fv expects
// with `transpose=false`. We avoid pulling in gl-matrix; the four operations we need are
// short enough to inline.

function mat4Identity(out) {
    out[0]=1; out[1]=0; out[2]=0; out[3]=0;
    out[4]=0; out[5]=1; out[6]=0; out[7]=0;
    out[8]=0; out[9]=0; out[10]=1; out[11]=0;
    out[12]=0; out[13]=0; out[14]=0; out[15]=1;
}

// out = a * b. `out` may alias either input.
function mat4Mul(out, a, b) {
    const a00=a[0],a01=a[1],a02=a[2],a03=a[3];
    const a10=a[4],a11=a[5],a12=a[6],a13=a[7];
    const a20=a[8],a21=a[9],a22=a[10],a23=a[11];
    const a30=a[12],a31=a[13],a32=a[14],a33=a[15];
    let b0=b[0],b1=b[1],b2=b[2],b3=b[3];
    out[0]=b0*a00+b1*a10+b2*a20+b3*a30;
    out[1]=b0*a01+b1*a11+b2*a21+b3*a31;
    out[2]=b0*a02+b1*a12+b2*a22+b3*a32;
    out[3]=b0*a03+b1*a13+b2*a23+b3*a33;
    b0=b[4]; b1=b[5]; b2=b[6]; b3=b[7];
    out[4]=b0*a00+b1*a10+b2*a20+b3*a30;
    out[5]=b0*a01+b1*a11+b2*a21+b3*a31;
    out[6]=b0*a02+b1*a12+b2*a22+b3*a32;
    out[7]=b0*a03+b1*a13+b2*a23+b3*a33;
    b0=b[8]; b1=b[9]; b2=b[10]; b3=b[11];
    out[8]=b0*a00+b1*a10+b2*a20+b3*a30;
    out[9]=b0*a01+b1*a11+b2*a21+b3*a31;
    out[10]=b0*a02+b1*a12+b2*a22+b3*a32;
    out[11]=b0*a03+b1*a13+b2*a23+b3*a33;
    b0=b[12]; b1=b[13]; b2=b[14]; b3=b[15];
    out[12]=b0*a00+b1*a10+b2*a20+b3*a30;
    out[13]=b0*a01+b1*a11+b2*a21+b3*a31;
    out[14]=b0*a02+b1*a12+b2*a22+b3*a32;
    out[15]=b0*a03+b1*a13+b2*a23+b3*a33;
}

function mat4Translate(out, x, y, z) {
    mat4Identity(out);
    out[12]=x; out[13]=y; out[14]=z;
}

function mat4Scale(out, sx, sy, sz) {
    mat4Identity(out);
    out[0]=sx; out[5]=sy; out[10]=sz;
}

function mat4RotateX(out, angle) {
    const c = Math.cos(angle), s = Math.sin(angle);
    mat4Identity(out);
    out[5]=c;  out[6]=s;
    out[9]=-s; out[10]=c;
}

function mat4RotateY(out, angle) {
    const c = Math.cos(angle), s = Math.sin(angle);
    mat4Identity(out);
    out[0]=c;  out[2]=-s;
    out[8]=s;  out[10]=c;
}

// Orthographic projection that flips the y axis so canvas-down coordinates map to clip-up.
function mat4Ortho(out, left, right, bottom, top, near, far) {
    const lr = 1 / (left - right);
    const bt = 1 / (bottom - top);
    const nf = 1 / (near - far);
    out[0] = -2 * lr;        out[1] = 0; out[2] = 0; out[3] = 0;
    out[4] = 0; out[5] = -2 * bt;        out[6] = 0; out[7] = 0;
    out[8] = 0; out[9] = 0; out[10] = 2 * nf;        out[11] = 0;
    out[12] = (left + right) * lr;
    out[13] = (top + bottom) * bt;
    out[14] = (far + near) * nf;
    out[15] = 1;
}

// MVP = ortho * rotateX(pitch) * rotateY(yaw) * scale(zoom) * translate(pan) * translate(-canvasCentre)
//
// At pitch=0, yaw=0, zoom=1, pan=(0,0) the composed matrix maps (0,0) → clip (-1, +1) and
// (width, height) → clip (+1, -1), which is exactly the original fullscreen-quad layout.
function composeMvp(out, w, h, cam) {
    const cx = w / 2, cy = h / 2;
    // Generous depth range: the most you can possibly tilt + scale by sqrt(2) is well within
    // [-maxDim, +maxDim] world units, so the ortho frustum never near-clips the geometry.
    const maxDim = Math.max(w, h) * 2;
    mat4Ortho(out, -cx, cx, cy, -cy, -maxDim, maxDim); // y flipped: bottom=cy, top=-cy
    mat4RotateX(tmpMat, cam.pitchRad);
    mat4Mul(out, out, tmpMat);
    mat4RotateY(tmpMat, cam.yawRad);
    mat4Mul(out, out, tmpMat);
    mat4Scale(tmpMat, cam.zoom, cam.zoom, cam.zoom);
    mat4Mul(out, out, tmpMat);
    mat4Translate(tmpMat, cam.panX, cam.panY, 0);
    mat4Mul(out, out, tmpMat);
    mat4Translate(tmpMat, -cx, -cy, 0);
    mat4Mul(out, out, tmpMat);
}
