/*
 * Orleans.Lattice Explorer AppKit, protocol v1: the in-frame bootstrap.
 *
 * This file runs inside a Lattice App's sandboxed, opaque-origin frame. It is
 * the only script frame.html loads, and it:
 *
 *   1. posts { type: "lattice.ready", protocol } to its parent;
 *   2. accepts exactly one "lattice.hello" from the parent carrying one
 *      transferred MessagePort, and then ignores every later window message;
 *   3. receives the app's bundle over that port, exactly once;
 *   4. re-verifies every asset's SHA-256 digest and the bundle digest with
 *      Web Crypto (defence in depth - the host already verified them);
 *   5. turns each asset into a blob: URL, inserts the entry fragment into
 *      <body>, appends the stylesheets, then loads the scripts one at a time
 *      in manifest order, classic or module as declared;
 *   6. exposes globalThis.lattice, the app's only way to reach the host,
 *      before the first app script runs.
 *
 * On any failure it replaces the frame's content with plain text and posts
 * "lattice.failed" with a code. It never evaluates strings as code, never
 * navigates, and assigns innerHTML exactly once: the verified entry fragment.
 *
 * Plain ES2022 with no dependencies and no build step. Plain ASCII only: the
 * repository's hygiene gates scan this file. The message names and bounds are
 * mirrored by Orleans.Lattice.Explorer.AppKit.AppKitProtocol and the protocol
 * schema, and tests hold the three together.
 */
(function () {
  "use strict";

  const PROTOCOL = 1;

  const MESSAGE = Object.freeze({
    ready: "lattice.ready",
    hello: "lattice.hello",
    bundle: "lattice.bundle",
    loaded: "lattice.loaded",
    failed: "lattice.failed"
  });

  const EVENT = Object.freeze({
    contextChanged: "context.changed",
    navChanged: "nav.changed",
    revoked: "lattice.revoked"
  });

  const EVENTS = Object.freeze([EVENT.contextChanged, EVENT.navChanged, EVENT.revoked]);

  const OPERATIONS = Object.freeze([
    "context.read", "context.user", "data.read", "data.write", "data.delete", "nav.sync", "ui.notify"
  ]);

  const ERROR = Object.freeze({
    denied: "denied",
    notFound: "not_found",
    invalid: "invalid",
    tooLarge: "too_large",
    rateLimited: "rate_limited",
    unavailable: "unavailable",
    conflict: "conflict"
  });

  const ERROR_CODES = Object.freeze(Object.values(ERROR));

  const FAILURE = Object.freeze({
    protocolUnsupported: "protocol_unsupported",
    bundleMalformed: "bundle_malformed",
    bundleTooLarge: "bundle_too_large",
    assetMissing: "asset_missing",
    digestMismatch: "digest_mismatch",
    bundleDigestMismatch: "bundle_digest_mismatch",
    cryptoUnavailable: "crypto_unavailable",
    loadFailed: "load_failed",
    internal: "internal"
  });

  const LIMIT = Object.freeze({
    maxAssets: 256,
    maxAssetBytes: 2 * 1024 * 1024,
    maxBundleBytes: 16 * 1024 * 1024,
    maxAssetPathLength: 256,
    maxValueBytes: 64 * 1024,
    maxRequestBytes: 128 * 1024,
    maxPageSize: 200,
    maxNotifyLength: 200,
    maxKeyLength: 1024,
    maxTreeNameLength: 128,
    maxPathLength: 1024,
    maxContinuationLength: 4096,
    defaultTimeoutMs: 30000,
    maxTimeoutMs: 300000
  });

  const MEDIA = Object.freeze({
    html: "text/html",
    css: "text/css",
    javascript: "text/javascript"
  });

  const ALLOWED_MEDIA_TYPES = Object.freeze([
    "text/html", "text/css", "text/javascript", "image/svg+xml", "image/png", "image/webp", "font/woff2",
    "application/json"
  ]);

  const THEMES = Object.freeze(["paper", "board"]);
  const CONTRASTS = Object.freeze(["standard", "more"]);
  const DENSITIES = Object.freeze(["comfortable", "compact"]);

  const ASSET_PATH = /^[a-z0-9._-]+(?:\/[a-z0-9._-]+)*$/;
  const DOT_SEGMENT = /(?:^|\/)\.{1,2}(?:\/|$)/;
  const DIGEST = /^[0-9a-f]{64}$/;
  const TREE_NAME = /^[a-z][a-z0-9_-]*$/;
  const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;

  /** A structured error from the bridge, with a code from the protocol's closed set. */
  class LatticeError extends Error {
    constructor(code, message) {
      super(typeof message === "string" && message.length > 0 ? message : code);
      this.name = "LatticeError";
      this.code = ERROR_CODES.includes(code) ? code : ERROR.unavailable;
    }
  }

  class BootFailure extends Error {
    constructor(code, message) {
      super(message);
      this.name = "BootFailure";
      this.code = code;
    }
  }

  const state = {
    port: null,
    helloAccepted: false,
    bundleReceived: false,
    live: false,
    failed: false,
    revoked: false,
    nextId: 1,
    pending: new Map(),
    handlers: new Map(EVENTS.map(function (name) { return [name, new Set()]; })),
    urls: new Map()
  };

  let resolveReady;
  let rejectReady;
  const ready = new Promise(function (resolve, reject) {
    resolveReady = resolve;
    rejectReady = reject;
  });
  // The kit settles ready on failure too; a page that never awaits it should not see an unhandled rejection.
  ready.catch(function () { });

  function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  function isString(value, minLength, maxLength) {
    return typeof value === "string" && value.length >= minLength && value.length <= maxLength;
  }

  function utf8Length(text) {
    return new TextEncoder().encode(text).length;
  }

  function hex(buffer) {
    const bytes = new Uint8Array(buffer);
    let text = "";
    for (let i = 0; i < bytes.length; i++) {
      text += bytes[i].toString(16).padStart(2, "0");
    }
    return text;
  }

  function asBytes(value) {
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    return null;
  }

  function reportHandlerError(error) {
    if (typeof globalThis.reportError === "function") {
      globalThis.reportError(error);
    }
  }

  // ---------------------------------------------------------------- rendering

  function renderText(text, role) {
    const notice = document.createElement("p");
    notice.className = "lt-app-notice";
    notice.setAttribute("role", role);
    notice.textContent = text;
    document.body.replaceChildren(notice);
  }

  function applyAppearance(appearance) {
    const source = isRecord(appearance) ? appearance : {};
    const theme = THEMES.includes(source.theme) ? source.theme : "paper";
    const contrast = CONTRASTS.includes(source.contrast) ? source.contrast : "standard";
    const density = DENSITIES.includes(source.density) ? source.density : "comfortable";
    const reducedMotion = source.reducedMotion === true;
    const root = document.documentElement;
    root.setAttribute("data-theme", theme);
    root.setAttribute("data-bs-theme", theme === "board" ? "dark" : "light");
    root.setAttribute("data-contrast", contrast);
    root.setAttribute("data-density", density);
    root.setAttribute("data-reduced-motion", reducedMotion ? "true" : "false");
    return Object.freeze({ theme: theme, contrast: contrast, density: density, reducedMotion: reducedMotion });
  }

  // ---------------------------------------------------------------- lifecycle

  function postControl(message) {
    if (state.port !== null) {
      state.port.postMessage(message);
    } else if (window.parent !== window) {
      window.parent.postMessage(message, "*");
    }
  }

  function rejectPending(code, message) {
    const pending = Array.from(state.pending.values());
    state.pending.clear();
    for (const entry of pending) {
      clearTimeout(entry.timer);
      entry.reject(new LatticeError(code, message));
    }
  }

  function closePort() {
    if (state.port !== null) {
      state.port.onmessage = null;
      state.port.close();
    }
  }

  function fail(code, message) {
    if (state.failed || state.revoked) {
      return;
    }
    state.failed = true;
    state.live = false;
    renderText("This app could not be loaded (" + code + ").", "alert");
    rejectReady(new LatticeError(ERROR.unavailable, "The app could not be loaded: " + code + "."));
    rejectPending(ERROR.unavailable, "The app could not be loaded.");
    postControl({ type: MESSAGE.failed, protocol: PROTOCOL, code: code, message: String(message) });
    closePort();
  }

  function revoke(data) {
    if (state.revoked || state.failed) {
      return;
    }
    state.revoked = true;
    state.live = false;
    rejectReady(new LatticeError(ERROR.unavailable, "The Explorer closed this app."));
    rejectPending(ERROR.unavailable, "The Explorer closed this app.");
    const reason = isRecord(data) && typeof data.reason === "string" ? data.reason : "closed";
    emit(EVENT.revoked, Object.freeze({ reason: reason }));
    closePort();
    renderText("The Explorer closed this app.", "status");
  }

  function emit(name, data) {
    const handlers = Array.from(state.handlers.get(name));
    for (const handler of handlers) {
      try {
        handler(data);
      } catch (error) {
        reportHandlerError(error);
      }
    }
  }

  // ---------------------------------------------------------------- the bundle

  function validateBundle(message) {
    if (message.protocol !== PROTOCOL) {
      throw new BootFailure(FAILURE.protocolUnsupported, "The bundle names an unsupported protocol version.");
    }
    const bundle = message.bundle;
    if (!isRecord(bundle) || !isRecord(bundle.assets) || typeof bundle.entry !== "string" ||
        (bundle.styles !== undefined && !Array.isArray(bundle.styles)) ||
        (bundle.scripts !== undefined && !Array.isArray(bundle.scripts)) ||
        typeof bundle.bundleDigest !== "string" || !DIGEST.test(bundle.bundleDigest)) {
      throw new BootFailure(FAILURE.bundleMalformed, "The bundle message is malformed.");
    }

    const paths = Object.keys(bundle.assets);
    if (paths.length === 0 || paths.length > LIMIT.maxAssets) {
      throw new BootFailure(FAILURE.bundleTooLarge, "The bundle carries no assets or too many assets.");
    }

    const assets = new Map();
    let total = 0;
    for (const path of paths) {
      const asset = bundle.assets[path];
      if (path.length > LIMIT.maxAssetPathLength || !ASSET_PATH.test(path) || DOT_SEGMENT.test(path) ||
          !isRecord(asset) || !ALLOWED_MEDIA_TYPES.includes(asset.mediaType) ||
          typeof asset.digest !== "string" || !DIGEST.test(asset.digest)) {
        throw new BootFailure(FAILURE.bundleMalformed, "A bundle asset is malformed.");
      }
      const bytes = asBytes(asset.bytes);
      if (bytes === null) {
        throw new BootFailure(FAILURE.bundleMalformed, "A bundle asset carries no bytes.");
      }
      if (bytes.byteLength > LIMIT.maxAssetBytes) {
        throw new BootFailure(FAILURE.bundleTooLarge, "A bundle asset is too large.");
      }
      total += bytes.byteLength;
      if (total > LIMIT.maxBundleBytes) {
        throw new BootFailure(FAILURE.bundleTooLarge, "The bundle is too large.");
      }
      assets.set(path, { mediaType: asset.mediaType, digest: asset.digest, bytes: bytes });
    }

    function reference(path, mediaType) {
      if (typeof path !== "string") {
        throw new BootFailure(FAILURE.bundleMalformed, "A bundle reference is not a path.");
      }
      const asset = assets.get(path);
      if (asset === undefined) {
        throw new BootFailure(FAILURE.assetMissing, "The bundle does not carry a referenced asset.");
      }
      if (asset.mediaType !== mediaType) {
        throw new BootFailure(FAILURE.bundleMalformed, "A referenced asset has the wrong media type.");
      }
      return path;
    }

    const entry = reference(bundle.entry, MEDIA.html);
    const styles = (bundle.styles || []).map(function (path) { return reference(path, MEDIA.css); });
    const scripts = (bundle.scripts || []).map(function (script) {
      if (!isRecord(script) || typeof script.module !== "boolean") {
        throw new BootFailure(FAILURE.bundleMalformed, "A bundle script is malformed.");
      }
      return { path: reference(script.path, MEDIA.javascript), module: script.module };
    });

    return { entry: entry, styles: styles, scripts: scripts, assets: assets, bundleDigest: bundle.bundleDigest };
  }

  async function verifyDigests(bundle) {
    if (!globalThis.crypto || !globalThis.crypto.subtle || typeof globalThis.crypto.subtle.digest !== "function") {
      throw new BootFailure(FAILURE.cryptoUnavailable, "Web Crypto is not available in this frame.");
    }
    const subtle = globalThis.crypto.subtle;
    for (const [, asset] of bundle.assets) {
      const actual = hex(await subtle.digest("SHA-256", asset.bytes));
      if (actual !== asset.digest) {
        throw new BootFailure(FAILURE.digestMismatch, "A bundle asset does not match its digest.");
      }
    }
    // The bundle digest, exactly as Orleans.Lattice.Apps.AppUiBundle.ComputeBundleDigest defines it: the
    // SHA-256 of "path NUL digest LF" for every asset in ordinal path order. Paths are ASCII, so the
    // default code-unit sort is ordinal.
    const lines = Array.from(bundle.assets.keys()).sort().map(function (path) {
      return path + "\u0000" + bundle.assets.get(path).digest + "\n";
    });
    const actualBundle = hex(await subtle.digest("SHA-256", new TextEncoder().encode(lines.join(""))));
    if (actualBundle !== bundle.bundleDigest) {
      throw new BootFailure(FAILURE.bundleDigestMismatch, "The bundle assets do not match the bundle digest.");
    }
  }

  function decodeEntry(bytes) {
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch (error) {
      throw new BootFailure(FAILURE.bundleMalformed, "The entry fragment is not UTF-8.");
    }
  }

  function awaitLoad(element, failure) {
    return new Promise(function (resolve, reject) {
      element.addEventListener("load", function () { resolve(); }, { once: true });
      element.addEventListener("error", function () { reject(new BootFailure(FAILURE.loadFailed, failure)); }, { once: true });
    });
  }

  async function materialise(message) {
    const bundle = validateBundle(message);
    await verifyDigests(bundle);
    if (state.revoked || state.failed) {
      return;
    }

    const appearance = applyAppearance(message.appearance);
    for (const [path, asset] of bundle.assets) {
      state.urls.set(path, URL.createObjectURL(new Blob([asset.bytes], { type: asset.mediaType })));
    }
    const fragment = decodeEntry(bundle.assets.get(bundle.entry).bytes);

    state.live = true;
    // The only innerHTML assignment in the kit: the entry fragment, whose digest was verified above. Any
    // script element or inline handler in it is inert, because the frame's policy forbids inline script.
    document.body.innerHTML = fragment;

    const styleLoads = bundle.styles.map(function (path) {
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = state.urls.get(path);
      const loaded = awaitLoad(link, "A bundle stylesheet failed to load.");
      document.head.append(link);
      return loaded;
    });
    await Promise.all(styleLoads);
    if (!state.live) {
      return;
    }

    resolveReady(appearance);

    for (const script of bundle.scripts) {
      const element = document.createElement("script");
      if (script.module) {
        element.type = "module";
      }
      element.async = false;
      element.src = state.urls.get(script.path);
      const loaded = awaitLoad(element, "A bundle script failed to load.");
      document.body.append(element);
      await loaded;
      if (!state.live) {
        return;
      }
    }

    postControl({ type: MESSAGE.loaded, protocol: PROTOCOL });
  }

  function receiveBundle(message) {
    if (state.bundleReceived || state.failed || state.revoked) {
      return;
    }
    state.bundleReceived = true;
    materialise(message).catch(function (error) {
      if (error instanceof BootFailure) {
        fail(error.code, error.message);
      } else {
        fail(FAILURE.internal, "The bootstrap failed.");
      }
    });
  }

  // ---------------------------------------------------------------- the port

  function receiveResponse(data) {
    if (!Number.isSafeInteger(data.id)) {
      return;
    }
    const entry = state.pending.get(data.id);
    if (entry === undefined) {
      return;
    }
    state.pending.delete(data.id);
    clearTimeout(entry.timer);
    if (data.ok === true) {
      entry.resolve(data.result === undefined ? {} : data.result);
    } else if (data.ok === false && isRecord(data.error)) {
      entry.reject(new LatticeError(data.error.code, typeof data.error.message === "string" ? data.error.message : ""));
    } else {
      entry.reject(new LatticeError(ERROR.unavailable, "The host sent a malformed response."));
    }
  }

  function onPortMessage(event) {
    const data = event.data;
    if (!isRecord(data)) {
      return;
    }
    if (typeof data.type !== "string") {
      receiveResponse(data);
      return;
    }
    switch (data.type) {
      case MESSAGE.bundle:
        receiveBundle(data);
        return;
      case EVENT.contextChanged:
        if (state.live) {
          emit(EVENT.contextChanged, applyAppearance(data.data));
        }
        return;
      case EVENT.navChanged:
        if (state.live && isRecord(data.data) && isString(data.data.path, 1, LIMIT.maxPathLength)) {
          emit(EVENT.navChanged, Object.freeze({ path: data.data.path }));
        }
        return;
      case EVENT.revoked:
        revoke(data.data);
        return;
      default:
        return;
    }
  }

  function onWindowMessage(event) {
    if (state.helloAccepted || event.source !== window.parent || window.parent === window) {
      return;
    }
    const data = event.data;
    if (!isRecord(data) || data.type !== MESSAGE.hello || !event.ports || event.ports.length !== 1) {
      return;
    }
    // Exactly one hello: from here on every window message is ignored, whoever sends it.
    state.helloAccepted = true;
    window.removeEventListener("message", onWindowMessage);
    state.port = event.ports[0];
    state.port.onmessage = onPortMessage;
    if (data.protocol !== PROTOCOL) {
      fail(FAILURE.protocolUnsupported, "The host speaks an unsupported protocol version.");
    }
  }

  // ---------------------------------------------------------------- the lattice API

  function checkKey(value, minLength) {
    return isString(value, minLength, LIMIT.maxKeyLength);
  }

  function checkTree(value) {
    return isString(value, 1, LIMIT.maxTreeNameLength) && TREE_NAME.test(value);
  }

  function base64Length(value) {
    const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
    return value.length / 4 * 3 - padding;
  }

  function onlyKeys(args, allowed) {
    return Object.keys(args).every(function (key) { return allowed.includes(key); });
  }

  // Returns null when the arguments are valid, otherwise the error to reject with. The host validates
  // again; this only saves a round trip and gives app code an immediate, typed answer.
  function validateArgs(op, args) {
    const invalid = function (message) { return new LatticeError(ERROR.invalid, message); };
    switch (op) {
      case "context.read":
      case "context.user":
        return onlyKeys(args, []) ? null : invalid(op + " takes no arguments.");
      case "nav.sync":
        if (!onlyKeys(args, ["path"]) || !isString(args.path, 1, LIMIT.maxPathLength) ||
            !args.path.startsWith("/") || CONTROL_CHARACTER.test(args.path)) {
          return invalid("nav.sync takes { path }, an absolute path of at most " + LIMIT.maxPathLength + " characters.");
        }
        return null;
      case "ui.notify":
        if (!onlyKeys(args, ["text"]) || typeof args.text !== "string") {
          return invalid("ui.notify takes { text }.");
        }
        if (args.text.length === 0 || args.text.length > LIMIT.maxNotifyLength) {
          return new LatticeError(ERROR.tooLarge, "ui.notify text must be 1 to " + LIMIT.maxNotifyLength + " characters.");
        }
        return null;
      default:
        break;
    }

    if (!checkTree(args.tree)) {
      return invalid(op + " needs a logical tree name.");
    }
    const action = args.action;
    if (op === "data.read" && action === "get") {
      return onlyKeys(args, ["action", "tree", "key"]) && checkKey(args.key, 1) ? null : invalid("get takes { action, tree, key }.");
    }
    if (op === "data.read" && action === "scan") {
      if (!onlyKeys(args, ["action", "tree", "prefix", "pageSize", "continuation"]) || !checkKey(args.prefix, 0)) {
        return invalid("scan takes { action, tree, prefix, pageSize?, continuation? }.");
      }
      if (args.pageSize !== undefined &&
          (!Number.isInteger(args.pageSize) || args.pageSize < 1 || args.pageSize > LIMIT.maxPageSize)) {
        return invalid("scan pageSize must be 1 to " + LIMIT.maxPageSize + ".");
      }
      if (args.continuation !== undefined && args.continuation !== null &&
          !isString(args.continuation, 1, LIMIT.maxContinuationLength)) {
        return invalid("scan continuation must be the token a previous page returned.");
      }
      return null;
    }
    if (op === "data.write" && action === "set") {
      if (!onlyKeys(args, ["action", "tree", "key", "value"]) || !checkKey(args.key, 1) ||
          typeof args.value !== "string" || args.value.length % 4 !== 0 || !BASE64.test(args.value)) {
        return invalid("set takes { action, tree, key, value } with a base64 value.");
      }
      if (base64Length(args.value) > LIMIT.maxValueBytes) {
        return new LatticeError(ERROR.tooLarge, "A value may be at most " + LIMIT.maxValueBytes + " bytes.");
      }
      return null;
    }
    if (op === "data.delete" && action === "delete") {
      return onlyKeys(args, ["action", "tree", "key"]) && checkKey(args.key, 1) ? null : invalid("delete takes { action, tree, key }.");
    }
    return invalid(op + " does not support that action.");
  }

  function request(op, args, options) {
    return new Promise(function (resolve, reject) {
      if (!OPERATIONS.includes(op)) {
        reject(new LatticeError(ERROR.invalid, "Unknown operation."));
        return;
      }
      if (!state.live || state.port === null) {
        reject(new LatticeError(ERROR.unavailable, "The bridge is not connected."));
        return;
      }
      const values = args === undefined ? {} : args;
      if (!isRecord(values)) {
        reject(new LatticeError(ERROR.invalid, "Arguments must be an object."));
        return;
      }
      const timeoutMs = isRecord(options) && options.timeoutMs !== undefined ? options.timeoutMs : LIMIT.defaultTimeoutMs;
      if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > LIMIT.maxTimeoutMs) {
        reject(new LatticeError(ERROR.invalid, "timeoutMs must be 1 to " + LIMIT.maxTimeoutMs + "."));
        return;
      }

      let json;
      try {
        json = JSON.stringify({ id: state.nextId, op: op, args: values });
      } catch (error) {
        reject(new LatticeError(ERROR.invalid, "Arguments must be JSON data."));
        return;
      }
      // Round-trip through JSON so only plain data ever crosses the port.
      const envelope = JSON.parse(json);
      const problem = validateArgs(op, envelope.args);
      if (problem !== null) {
        reject(problem);
        return;
      }
      if (utf8Length(json) > LIMIT.maxRequestBytes) {
        reject(new LatticeError(ERROR.tooLarge, "The request is too large."));
        return;
      }

      const id = state.nextId++;
      const timer = setTimeout(function () {
        if (state.pending.delete(id)) {
          reject(new LatticeError(ERROR.unavailable, "The request timed out."));
        }
      }, timeoutMs);
      state.pending.set(id, { resolve: resolve, reject: reject, timer: timer });
      state.port.postMessage(envelope);
    });
  }

  function on(name, handler) {
    if (!EVENTS.includes(name)) {
      throw new LatticeError(ERROR.invalid, "Unknown event.");
    }
    if (typeof handler !== "function") {
      throw new LatticeError(ERROR.invalid, "The handler must be a function.");
    }
    const handlers = state.handlers.get(name);
    handlers.add(handler);
    return function unsubscribe() {
      handlers.delete(handler);
    };
  }

  function assetUrl(path) {
    const url = typeof path === "string" ? state.urls.get(path) : undefined;
    if (url === undefined) {
      throw new LatticeError(ERROR.notFound, "The bundle has no such asset.");
    }
    return url;
  }

  Object.defineProperty(globalThis, "lattice", {
    value: Object.freeze({
      protocol: PROTOCOL,
      ready: ready,
      request: request,
      on: on,
      assetUrl: assetUrl,
      LatticeError: LatticeError
    }),
    writable: false,
    enumerable: false,
    configurable: false
  });

  // ---------------------------------------------------------------- start

  window.addEventListener("message", onWindowMessage);
  if (window.parent === window) {
    renderText("This document runs only inside the Lattice Explorer.", "status");
  } else {
    window.parent.postMessage({ type: MESSAGE.ready, protocol: PROTOCOL }, "*");
  }
})();
