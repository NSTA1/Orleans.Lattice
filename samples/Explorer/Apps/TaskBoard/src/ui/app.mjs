// The task board's one module. It is self-contained: no imports, no network,
// no storage. The AppKit bootstrap defines globalThis.lattice before this runs,
// and every call below goes through the Explorer's bridge broker to the
// cluster, under the signed-in user's rights intersected with this app's
// consented grants. The board starts read-only and shows its write controls
// only when context.read reports a role that this app's manifest lets write
// (see WRITER_ROLES). The cluster still enforces every write, and any denied
// write drops the board back to read-only, so a hint that is wrong can only
// ever hide a control, never grant one.

const TREE = "tasks";
const PREFIX = "tasks/";
// The roles in manifest.json whose operations include Write and Delete on the
// tasks tree. A test keeps this list equal to the manifest.
const WRITER_ROLES = ["editor"];
const COLUMNS = ["todo", "doing", "done"];
const COLUMN_NAMES = { todo: "To do", doing: "Doing", done: "Done" };
const ID_PATTERN = /^[a-z0-9-]{1,64}$/;
const MAX_TITLE = 200;
const MAX_TASKS = 2000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const state = {
  tasks: new Map(),
  selected: null,
  wanted: null,
  canEdit: false,
  busy: false
};

function byId(id) {
  return document.getElementById(id);
}

function toBase64(text) {
  const bytes = encoder.encode(text);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

function fromBase64(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return decoder.decode(bytes);
}

function errorCode(error) {
  return error instanceof lattice.LatticeError ? error.code : "unavailable";
}

function setStatus(text) {
  byId("tb-status").textContent = text;
}

function notify(text) {
  lattice.request("ui.notify", { text: text.slice(0, 200) }).catch(function () { });
}

// Values are JSON written by this module, but the tree is shared with anyone
// holding an editor role, so every stored value is treated as untrusted input
// and rendered only as text.
function parseTask(key, base64) {
  const id = key.slice(PREFIX.length);
  if (!ID_PATTERN.test(id)) {
    return null;
  }
  try {
    const value = JSON.parse(fromBase64(base64));
    if (value === null || typeof value !== "object" || typeof value.title !== "string" ||
        !COLUMNS.includes(value.column)) {
      return null;
    }
    return {
      id: id,
      key: key,
      title: value.title.slice(0, MAX_TITLE),
      column: value.column,
      created: typeof value.created === "string" ? value.created : ""
    };
  } catch (error) {
    return null;
  }
}

function newId() {
  const bytes = new Uint8Array(6);
  crypto.getRandomValues(bytes);
  let suffix = "";
  for (let i = 0; i < bytes.length; i++) {
    suffix += bytes[i].toString(16).padStart(2, "0");
  }
  return Date.now().toString(36) + "-" + suffix;
}

function taskPath(id) {
  return id === null ? "/" : "/tasks/" + id;
}

function idFromPath(path) {
  const match = /^\/tasks\/([^/?#]+)$/.exec(path);
  return match !== null && ID_PATTERN.test(match[1]) ? match[1] : null;
}

// ---------------------------------------------------------------- rendering

function cardFor(task) {
  const item = document.createElement("li");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "lt-app-button lt-app-button--quiet tb-card";
  button.textContent = task.title;
  button.dataset.id = task.id;
  button.setAttribute("aria-current", task.id === state.selected ? "true" : "false");
  button.addEventListener("click", function () {
    select(task.id === state.selected ? null : task.id, true);
  });
  item.append(button);
  return item;
}

function renderColumns() {
  for (const column of COLUMNS) {
    const tasks = Array.from(state.tasks.values())
      .filter(function (task) { return task.column === column; })
      .sort(function (a, b) { return a.created < b.created ? -1 : a.created > b.created ? 1 : a.id < b.id ? -1 : 1; });
    const items = tasks.map(cardFor);
    if (items.length === 0) {
      const empty = document.createElement("li");
      empty.className = "tb-empty";
      empty.textContent = "No tasks.";
      items.push(empty);
    }
    byId("tb-cards-" + column).replaceChildren(...items);
    byId("tb-count-" + column).textContent = "(" + tasks.length + ")";
  }
}

function renderDetail() {
  const detail = byId("tb-detail");
  const task = state.selected === null ? undefined : state.tasks.get(state.selected);
  if (task === undefined) {
    detail.hidden = true;
    return;
  }
  detail.hidden = false;
  byId("tb-detail-title").textContent = task.title;
  byId("tb-detail-key").textContent = task.key;
  byId("tb-detail-column").textContent = "In " + COLUMN_NAMES[task.column] + ".";
  byId("tb-detail-actions").hidden = !state.canEdit;
  for (const button of byId("tb-detail-actions").querySelectorAll("[data-move]")) {
    button.hidden = button.dataset.move === task.column;
    button.disabled = state.busy;
  }
  byId("tb-delete").disabled = state.busy;
}

function renderAccess() {
  byId("tb-add").hidden = !state.canEdit;
  byId("tb-read-only").hidden = state.canEdit;
  byId("tb-add-button").disabled = state.busy;
}

function render() {
  renderAccess();
  renderColumns();
  renderDetail();
}

function showContext(context, appearance) {
  const material = appearance.theme === "board" ? "Board" : "Paper";
  byId("tb-context").textContent =
    context.slug + " " + context.version + (context.tenant ? " in " + context.tenant : "") + ", " + material + ".";
}

function showIcon() {
  try {
    const icon = document.createElement("img");
    icon.className = "tb-icon";
    icon.alt = "";
    icon.width = 32;
    icon.height = 32;
    icon.src = lattice.assetUrl("icon.svg");
    byId("tb-head").prepend(icon);
  } catch (error) {
    // The icon is decoration; the board works without it.
  }
}

// ---------------------------------------------------------------- selection

function select(id, sync) {
  // A deep link can arrive before the board has loaded; remember it until it can resolve.
  state.wanted = id;
  state.selected = id !== null && state.tasks.has(id) ? id : null;
  renderColumns();
  renderDetail();
  if (sync && state.selected !== null) {
    byId("tb-detail-title").focus();
  }
  if (sync) {
    lattice.request("nav.sync", { path: taskPath(state.selected) }).catch(function () { });
  }
}

// ---------------------------------------------------------------- data

async function loadAll() {
  const tasks = new Map();
  let continuation = null;
  do {
    const args = { action: "scan", tree: TREE, prefix: PREFIX, pageSize: 200 };
    if (continuation !== null) {
      args.continuation = continuation;
    }
    const page = await lattice.request("data.read", args);
    for (const entry of page.entries) {
      const task = parseTask(entry.key, entry.value);
      if (task !== null) {
        tasks.set(task.id, task);
      }
    }
    continuation = page.continuation || null;
  } while (continuation !== null && tasks.size < MAX_TASKS);
  state.tasks = tasks;
  state.selected = state.wanted !== null && tasks.has(state.wanted) ? state.wanted : null;
}

// context.read carries the caller's role names in this app. A host that
// predates the member omits it, and then no role may be inferred: the board
// stays read-only.
function canWrite(context) {
  if (context === null || typeof context !== "object" || !Array.isArray(context.roles)) {
    return false;
  }
  return context.roles.some(function (role) { return WRITER_ROLES.includes(role); });
}

async function writeTask(task) {
  const value = { title: task.title, column: task.column, created: task.created };
  await lattice.request("data.write", { action: "set", tree: TREE, key: task.key, value: toBase64(JSON.stringify(value)) });
}

async function mutate(action, failureText) {
  if (state.busy || !state.canEdit) {
    return;
  }
  state.busy = true;
  render();
  try {
    await action();
  } catch (error) {
    const code = errorCode(error);
    if (code === "denied") {
      state.canEdit = false;
    }
    setStatus(failureText + " (" + code + ").");
    notify(failureText + " (" + code + ").");
  } finally {
    state.busy = false;
    render();
  }
}

function addTask() {
  const input = byId("tb-add-title");
  const title = input.value.trim().slice(0, MAX_TITLE);
  if (title.length === 0) {
    input.setAttribute("aria-invalid", "true");
    setStatus("Give the task a title first.");
    input.focus();
    return;
  }
  input.removeAttribute("aria-invalid");
  mutate(async function () {
    const id = newId();
    const task = { id: id, key: PREFIX + id, title: title, column: "todo", created: new Date().toISOString() };
    await writeTask(task);
    state.tasks.set(id, task);
    input.value = "";
    setStatus("Added \"" + title + "\" to To do.");
  }, "The task could not be added");
}

async function moveSelected(column) {
  const task = state.selected === null ? undefined : state.tasks.get(state.selected);
  if (task === undefined || !COLUMNS.includes(column) || task.column === column) {
    return;
  }
  await mutate(async function () {
    const moved = Object.assign({}, task, { column: column });
    await writeTask(moved);
    state.tasks.set(moved.id, moved);
    setStatus("Moved \"" + moved.title + "\" to " + COLUMN_NAMES[column] + ".");
  }, "The task could not be moved");
  // The pressed button is hidden once the task is in that column, so keep focus in the detail.
  byId("tb-detail-title").focus();
}

async function deleteSelected() {
  const task = state.selected === null ? undefined : state.tasks.get(state.selected);
  if (task === undefined) {
    return;
  }
  await mutate(async function () {
    await lattice.request("data.delete", { action: "delete", tree: TREE, key: task.key });
    state.tasks.delete(task.id);
    state.selected = null;
    state.wanted = null;
    lattice.request("nav.sync", { path: taskPath(null) }).catch(function () { });
    setStatus("Deleted \"" + task.title + "\".");
  }, "The task could not be deleted");
  byId(state.canEdit ? "tb-add-title" : "tb-refresh").focus();
}

async function refresh() {
  try {
    await loadAll();
    setStatus("");
  } catch (error) {
    setStatus("The board could not be read (" + errorCode(error) + ").");
  }
  render();
}

// ---------------------------------------------------------------- wiring

byId("tb-add-button").addEventListener("click", addTask);
byId("tb-add-title").addEventListener("keydown", function (event) {
  if (event.key === "Enter") {
    event.preventDefault();
    addTask();
  }
});
for (const button of byId("tb-detail-actions").querySelectorAll("[data-move]")) {
  button.addEventListener("click", function () {
    moveSelected(button.dataset.move);
  });
}
byId("tb-delete").addEventListener("click", deleteSelected);
byId("tb-close").addEventListener("click", function () {
  select(null, true);
});
byId("tb-refresh").addEventListener("click", refresh);

lattice.on("nav.changed", function (data) {
  select(idFromPath(data.path), false);
});

lattice.on("lattice.revoked", function () {
  state.canEdit = false;
  state.busy = true;
});

async function start() {
  let context = null;
  try {
    const appearance = await lattice.ready;
    showIcon();
    context = await lattice.request("context.read");
    showContext(context, appearance);
    // The kit has already re-applied the Paper or Board attributes, so the
    // stylesheet follows by itself; this only keeps the label honest.
    lattice.on("context.changed", function (next) {
      showContext(context, next);
    });
    state.canEdit = canWrite(context);
    await refresh();
  } catch (error) {
    byId("tb-context").textContent = "The board could not start (" + errorCode(error) + ").";
    render();
  }
}

start();
