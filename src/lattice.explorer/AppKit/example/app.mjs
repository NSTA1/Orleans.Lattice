// The example app's one module. The AppKit bootstrap defines globalThis.lattice
// before this runs, and every call below goes through the Explorer's bridge
// broker to the cluster, under the signed-in user's rights intersected with
// this app's consented grants. Nothing here can reach a credential, another
// app, or a tree this app does not own.

const decoder = new TextDecoder();

function decode(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return decoder.decode(bytes);
}

function cell(text) {
  const td = document.createElement("td");
  td.textContent = text;
  return td;
}

async function showContext() {
  const context = await lattice.request("context.read");
  document.getElementById("notes-context").textContent =
    context.slug + " " + context.version + (context.tenant ? " in " + context.tenant : "");
}

async function refresh() {
  const rows = document.getElementById("notes-rows");
  const page = await lattice.request("data.read", { action: "scan", tree: "notes", prefix: "", pageSize: 50 });
  const children = page.entries.map(function (entry) {
    const tr = document.createElement("tr");
    tr.append(cell(entry.key), cell(decode(entry.value)));
    return tr;
  });
  if (children.length === 0) {
    const tr = document.createElement("tr");
    const td = cell("No notes yet.");
    td.colSpan = 2;
    td.className = "notes-empty";
    tr.append(td);
    children.push(tr);
  }
  rows.replaceChildren(...children);
}

async function start() {
  try {
    await lattice.ready;
    await showContext();
    await refresh();
  } catch (error) {
    const code = error instanceof lattice.LatticeError ? error.code : "unavailable";
    document.getElementById("notes-context").textContent = "The notes could not be read (" + code + ").";
  }
}

document.getElementById("notes-refresh").addEventListener("click", function () {
  refresh().catch(function (error) {
    lattice.request("ui.notify", { text: "Refresh failed: " + error.code }).catch(function () { });
  });
});

lattice.on("lattice.revoked", function () {
  document.getElementById("notes-refresh").disabled = true;
});

start();
