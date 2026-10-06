// The Explorer-side half of the Lattice App frame handshake (issue #3817, epic #3807 E4/E5).
//
// This module runs in the Explorer's own page, never inside a frame. It:
//   - counts the frame's load events once its bootstrap src is set;
//   - accepts one lattice.ready, only from that frame's contentWindow and only while
//     no second document has loaded, then transfers exactly one MessagePort with
//     lattice.hello;
//   - on any later load, closes the port, blanks and hides the frame, and tells .NET;
//   - relays port messages to .NET through one DotNetObjectReference as text, and
//     never evaluates anything the frame supplies.
// Replies and events come from .NET as JSON the host built; they are parsed, not run.

const HOST_PROTOCOL = 1;
const MAX_REQUEST_CHARS = 131072;
const MAX_CODE_CHARS = 64;
const frames = new Map();

export function attach(frameId, frame, dotnet) {
  if (typeof frameId !== 'string' || frames.has(frameId) || !(frame instanceof HTMLIFrameElement) || !dotnet) {
    return false;
  }

  const state = {
    frame,
    dotnet,
    loads: 0,
    port: null,
    hello: false,
    bundleSent: false,
    closed: false,
    staged: new Map(),
    onLoad: null,
    onMessage: null,
  };

  state.onLoad = () => {
    // The initial about:blank document, before .NET renders the bootstrap src, is not counted.
    if (state.closed || !frame.hasAttribute('src')) {
      return;
    }

    state.loads += 1;
    if (state.loads > 1) {
      teardown(frameId, state);
      notify(state, 'OnFrameReloaded');
    }
  };

  state.onMessage = (event) => {
    if (state.closed || event.source === null || event.source !== frame.contentWindow) {
      return;
    }

    const data = event.data;
    if (data === null || typeof data !== 'object') {
      return;
    }

    if (data.type === 'lattice.failed') {
      reportFailed(frameId, state, data);
      return;
    }

    if (state.hello || state.loads > 1 || data.type !== 'lattice.ready' || !Number.isSafeInteger(data.protocol)) {
      return;
    }

    state.hello = true;
    const channel = new MessageChannel();
    state.port = channel.port1;
    state.port.onmessage = (portEvent) => onPortMessage(frameId, state, portEvent);
    state.port.onmessageerror = () => {};
    frame.contentWindow.postMessage({ type: 'lattice.hello', protocol: HOST_PROTOCOL }, '*', [channel.port2]);
    notify(state, 'OnFrameReady', data.protocol);
  };

  frame.addEventListener('load', state.onLoad);
  window.addEventListener('message', state.onMessage);
  frames.set(frameId, state);
  return true;
}

export async function stageAsset(frameId, path, streamRef) {
  const state = frames.get(frameId);
  if (!state || state.closed || state.bundleSent || typeof path !== 'string') {
    return false;
  }

  const bytes = await streamRef.arrayBuffer();
  if (state.closed) {
    return false;
  }

  state.staged.set(path, bytes);
  return true;
}

export function sendBundle(frameId, messageJson) {
  const state = frames.get(frameId);
  if (!state || state.closed || !state.port || state.bundleSent) {
    return false;
  }

  const message = JSON.parse(messageJson);
  const transfer = [];
  for (const path of Object.keys(message.bundle.assets)) {
    const bytes = state.staged.get(path);
    if (!(bytes instanceof ArrayBuffer)) {
      return false;
    }

    message.bundle.assets[path].bytes = bytes;
    transfer.push(bytes);
  }

  state.staged.clear();
  state.bundleSent = true;
  state.port.postMessage(message, transfer);
  return true;
}

export function post(frameId, messageJson) {
  const state = frames.get(frameId);
  if (!state || state.closed || !state.port || typeof messageJson !== 'string') {
    return false;
  }

  state.port.postMessage(JSON.parse(messageJson));
  return true;
}

export function revoke(frameId, reason) {
  const state = frames.get(frameId);
  if (!state) {
    return false;
  }

  if (!state.closed && state.port) {
    state.port.postMessage({ type: 'lattice.revoked', data: { reason: String(reason) } });
  }

  teardown(frameId, state);
  return true;
}

export function detach(frameId) {
  const state = frames.get(frameId);
  if (!state) {
    return false;
  }

  teardown(frameId, state);
  return true;
}

export function focusAddressLine() {
  const target = document.querySelector('[data-lt-address-line]');
  if (target instanceof HTMLElement) {
    target.focus();
    return true;
  }

  return false;
}

// The appearance the Explorer's own page is drawn in, as the chrome resolved it on the
// document: the material (data-bs-theme, with "follow the system" already resolved),
// the contrast overlay (an explicit choice, or the platform's prefers-contrast), the
// density, and the platform's reduced-motion preference. Only names from the closed set
// are returned; .NET sanitises them again before any reaches a frame.
export function readAppearance() {
  const root = document.documentElement;
  const media = (query) => !!window.matchMedia && window.matchMedia(query).matches;
  const contrastChoice = root.getAttribute('data-lt-contrast');
  const contrast = contrastChoice === 'more' || contrastChoice === 'standard'
    ? contrastChoice
    : (media('(prefers-contrast: more)') ? 'more' : 'standard');
  return [
    root.getAttribute('data-bs-theme') === 'dark' ? 'board' : 'paper',
    contrast,
    root.getAttribute('data-lt-density') === 'compact' ? 'compact' : 'comfortable',
    media('(prefers-reduced-motion: reduce)') ? 'reduce' : 'full',
  ];
}

function onPortMessage(frameId, state, event) {
  if (state.closed) {
    return;
  }

  const data = event.data;
  if (data !== null && typeof data === 'object' && typeof data.type === 'string') {
    if (data.type === 'lattice.failed') {
      reportFailed(frameId, state, data);
    } else if (data.type === 'lattice.loaded') {
      notify(state, 'OnFrameLoaded');
    }

    return;
  }

  let text;
  try {
    text = JSON.stringify(data);
  } catch {
    return;
  }

  if (typeof text !== 'string' || text.length > MAX_REQUEST_CHARS) {
    return;
  }

  state.dotnet.invokeMethodAsync('OnPortMessage', text).then(
    (reply) => {
      if (!state.closed && state.port && typeof reply === 'string') {
        state.port.postMessage(JSON.parse(reply));
      }
    },
    () => {});
}

function reportFailed(frameId, state, data) {
  const code = typeof data.code === 'string' && data.code.length <= MAX_CODE_CHARS ? data.code : 'internal';
  teardown(frameId, state);
  notify(state, 'OnFrameFailed', code);
}

function notify(state, method, ...args) {
  state.dotnet.invokeMethodAsync(method, ...args).catch(() => {});
}

function teardown(frameId, state) {
  if (state.closed) {
    return;
  }

  state.closed = true;
  state.staged.clear();
  window.removeEventListener('message', state.onMessage);
  state.frame.removeEventListener('load', state.onLoad);
  if (state.port) {
    state.port.onmessage = null;
    state.port.close();
    state.port = null;
  }

  // Unload the frame's document and hide the element. Blazor removes the element itself
  // when .NET re-renders the failure state, so it is not detached from the DOM here.
  state.frame.hidden = true;
  state.frame.setAttribute('src', 'about:blank');
  frames.delete(frameId);
}
