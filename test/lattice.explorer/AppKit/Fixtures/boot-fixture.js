/*
 * AppKit bootstrap fixture (issue #3814): the host half of the frame protocol,
 * reduced to what a browser test needs to prove boot.js's ordering and failure
 * paths. See boot-fixture.html for how a runner drives it.
 *
 * Plain ES2022, no dependencies. Plain ASCII only: the repository's hygiene
 * gates scan this file.
 */
(function () {
  "use strict";

  const SCENARIOS = {
    "order": { outcome: "loaded", notifications: ["classic-1:styled:true", "module-2:function", "classic-3"] },
    "second-hello": { outcome: "loaded", notifications: ["classic-1:styled:true", "module-2:function", "classic-3"], strayMessages: 0 },
    "revoked": { outcome: "loaded", notifications: ["classic-1:styled:true", "module-2:function", "classic-3"], revoke: true },
    "digest-mismatch": { outcome: "failed", code: "digest_mismatch", notifications: [] },
    "bundle-digest-mismatch": { outcome: "failed", code: "bundle_digest_mismatch", notifications: [] },
    "asset-missing": { outcome: "failed", code: "asset_missing", notifications: [] },
    "malformed": { outcome: "failed", code: "bundle_malformed", notifications: [] },
    "protocol": { outcome: "failed", code: "protocol_unsupported", notifications: [] }
  };

  const ASSETS = {
    "index.html": { mediaType: "text/html", text: "<div id=\"probe\">probe</div>\n" },
    "style.css": { mediaType: "text/css", text: "#probe { --probe: styled; }\n" },
    "one.js": {
      mediaType: "text/javascript",
      text: "(function () {\n" +
        "  var styled = getComputedStyle(document.getElementById(\"probe\")).getPropertyValue(\"--probe\").trim();\n" +
        "  var blob = lattice.assetUrl(\"one.js\").indexOf(\"blob:\") === 0;\n" +
        "  lattice.request(\"ui.notify\", { text: \"classic-1:\" + styled + \":\" + blob });\n" +
        "})();\n"
    },
    "two.mjs": {
      mediaType: "text/javascript",
      text: "lattice.request(\"ui.notify\", { text: \"module-2:\" + typeof lattice.request });\n"
    },
    "three.js": {
      mediaType: "text/javascript",
      text: "lattice.request(\"ui.notify\", { text: \"classic-3\" });\n"
    }
  };

  const params = new URLSearchParams(window.location.search);
  const frameUrl = params.get("frame") || "/_apps/frame/v1/frame.html";
  const scenarioName = params.get("scenario") || "order";
  const expected = SCENARIOS[scenarioName];
  const root = document.documentElement;
  root.setAttribute("data-scenarios", Object.keys(SCENARIOS).join(" "));

  const record = {
    scenario: scenarioName,
    outcome: null,
    code: null,
    notifications: [],
    strayMessages: 0,
    readyCount: 0
  };
  let finished = false;

  function finish() {
    if (finished) {
      return;
    }
    finished = true;
    const pass = expected !== undefined &&
      record.outcome === expected.outcome &&
      (expected.code === undefined || record.code === expected.code) &&
      JSON.stringify(record.notifications) === JSON.stringify(expected.notifications) &&
      (expected.strayMessages === undefined || record.strayMessages === expected.strayMessages) &&
      record.readyCount === 1;
    root.setAttribute("data-result", JSON.stringify(record));
    root.setAttribute("data-pass", pass ? "true" : "false");
    root.setAttribute("data-state", "done");
    document.getElementById("fixture-status").textContent = pass ? "Passed." : "Failed: " + JSON.stringify(record);
  }

  function hex(buffer) {
    return Array.from(new Uint8Array(buffer), function (b) { return b.toString(16).padStart(2, "0"); }).join("");
  }

  async function sha256(bytes) {
    return hex(await crypto.subtle.digest("SHA-256", bytes));
  }

  async function buildBundle() {
    const encoder = new TextEncoder();
    const assets = {};
    for (const path of Object.keys(ASSETS)) {
      const bytes = encoder.encode(ASSETS[path].text);
      assets[path] = { mediaType: ASSETS[path].mediaType, digest: await sha256(bytes), bytes: bytes };
    }
    const lines = Object.keys(assets).sort().map(function (path) { return path + "\u0000" + assets[path].digest + "\n"; });
    const bundle = {
      entry: "index.html",
      styles: ["style.css"],
      scripts: [{ path: "one.js", module: false }, { path: "two.mjs", module: true }, { path: "three.js", module: false }],
      bundleDigest: await sha256(encoder.encode(lines.join(""))),
      assets: assets
    };

    switch (scenarioName) {
      case "digest-mismatch":
        assets["three.js"].bytes = encoder.encode("lattice.request(\"ui.notify\", { text: \"tampered\" });\n");
        break;
      case "bundle-digest-mismatch":
        bundle.bundleDigest = "0".repeat(64);
        break;
      case "asset-missing":
        bundle.scripts.push({ path: "absent.js", module: false });
        break;
      case "malformed":
        delete bundle.entry;
        break;
      default:
        break;
    }
    return bundle;
  }

  function bundleMessage(bundle) {
    return {
      type: "lattice.bundle",
      protocol: 1,
      appearance: { theme: "paper", contrast: "standard", density: "comfortable", reducedMotion: true },
      bundle: bundle
    };
  }

  function answer(port, request) {
    if (request.op === "ui.notify") {
      record.notifications.push(request.args.text);
      port.postMessage({ id: request.id, ok: true, result: {} });
    } else if (request.op === "context.read") {
      port.postMessage({
        id: request.id,
        ok: true,
        result: {
          slug: "fixture", version: "1.0.0", protocol: 1, theme: "paper", contrast: "standard",
          density: "comfortable", reducedMotion: true, tenant: null
        }
      });
    } else {
      port.postMessage({ id: request.id, ok: false, error: { code: "denied", message: "Not granted in the fixture." } });
    }
  }

  async function handshake(frame) {
    const channel = new MessageChannel();
    const port = channel.port1;
    port.onmessage = function (event) {
      const data = event.data;
      if (data === null || typeof data !== "object") {
        return;
      }
      if (data.type === "lattice.loaded") {
        record.outcome = "loaded";
        if (expected !== undefined && expected.revoke) {
          port.postMessage({ type: "lattice.revoked", data: { reason: "closed" } });
        }
        finish();
      } else if (data.type === "lattice.failed") {
        record.outcome = "failed";
        record.code = data.code;
        finish();
      } else if (typeof data.type !== "string" && Number.isSafeInteger(data.id)) {
        answer(port, data);
      }
    };

    const protocol = scenarioName === "protocol" ? 2 : 1;
    frame.contentWindow.postMessage({ type: "lattice.hello", protocol: protocol }, "*", [channel.port2]);

    const bundle = await buildBundle();
    if (scenarioName === "second-hello") {
      // A second hello with its own port, sent a bundle first: the frame must ignore both.
      const stray = new MessageChannel();
      stray.port1.onmessage = function () { record.strayMessages++; };
      frame.contentWindow.postMessage({ type: "lattice.hello", protocol: 1 }, "*", [stray.port2]);
      stray.port1.postMessage(bundleMessage(await buildBundle()));
    }
    port.postMessage(bundleMessage(bundle));
  }

  function start() {
    if (expected === undefined) {
      record.outcome = "unknown-scenario";
      finish();
      return;
    }
    const frame = document.createElement("iframe");
    frame.setAttribute("sandbox", "allow-scripts");
    frame.setAttribute("referrerpolicy", "no-referrer");
    frame.setAttribute("title", "AppKit fixture app");
    window.addEventListener("message", function (event) {
      if (event.source !== frame.contentWindow || event.data === null || typeof event.data !== "object") {
        return;
      }
      if (event.data.type === "lattice.ready" && event.data.protocol === 1) {
        record.readyCount++;
        if (record.readyCount === 1) {
          handshake(frame).catch(function (error) {
            record.outcome = "fixture-error:" + error;
            finish();
          });
        }
      } else if (event.data.type === "lattice.failed") {
        record.outcome = "failed";
        record.code = event.data.code;
        finish();
      }
    });
    frame.src = frameUrl;
    document.body.append(frame);
  }

  start();
})();
