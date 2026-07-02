const $ = (s) => document.querySelector(s);

const nodesInput = $("#nodes");
const topology = $("#topology");
const nodeCount = $("#node-count");
const healthDot = $("#health-dot");
const leaderChip = $("#leader-chip");
const segSlide = $("#seg-slide");
const form = $("#form");
const io = $("#io");
const keyInput = $("#key");
const valueInput = $("#value");
const valueField = $("#value-field");
const recentKeys = $("#recent-keys");
const execute = $("#execute");
const executeLabel = $("#execute-label");
const modeLabel = $("#mode-label");
const statusEl = $("#status");
const resultBox = $(".result__value");
const resultValue = $("#result-value");
const latencyEl = $("#latency");
const spark = $("#spark");
const leaderResult = $("#leader-result");
const opCount = $("#op-count");
const log = $("#log");

const DEFAULT_NODES = "127.0.0.1:7001, 127.0.0.1:7002, 127.0.0.1:7003";
const LABELS = { put: "commit put", get: "read key", delete: "delete key" };
const INDEX = { put: 0, get: 1, delete: 2 };
const TIMEOUT_MS = 8000;

let activeOp = "put";
let operations = 0;
let currentLeader = null;
const latencies = [];
const recent = new Set();

function nodeList() {
  return nodesInput.value.split(",").map((n) => n.trim()).filter(Boolean);
}

function renderTopology() {
  const nodes = nodeList();
  nodeCount.textContent = `${nodes.length} ${nodes.length === 1 ? "node" : "nodes"}`;
  topology.textContent = "";
  nodes.forEach((addr, i) => {
    const node = document.createElement("div");
    node.className = i === currentLeader ? "node is-leader" : "node";

    const disc = document.createElement("div");
    disc.className = "node__disc";
    disc.textContent = `N${i}`;

    const where = document.createElement("div");
    where.className = "node__addr";
    where.textContent = addr;

    const role = document.createElement("div");
    role.className = "node__role";
    role.textContent = i === currentLeader ? "leader" : "follower";

    node.append(disc, where, role);
    topology.append(node);
  });
}

function setLeader(index) {
  currentLeader = typeof index === "number" ? index : null;
  if (currentLeader === null) {
    leaderChip.textContent = "—";
    leaderResult.textContent = "—";
  } else {
    healthDot.classList.add("is-live");
    healthDot.classList.remove("is-offline");
    leaderChip.textContent = `N${currentLeader}`;
    leaderResult.textContent = `N${currentLeader}`;
  }
  renderTopology();
}

function leaderPulse() {
  const el = topology.querySelector(".node.is-leader");
  if (!el) return;
  el.classList.remove("pulse");
  void el.offsetWidth;
  el.classList.add("pulse");
}

function emitPackets() {
  const leaderDisc = topology.querySelector(".node.is-leader .node__disc");
  if (!leaderDisc) return;
  const base = topology.getBoundingClientRect();
  const from = leaderDisc.getBoundingClientRect();
  const sx = from.left - base.left + from.width / 2;
  const sy = from.top - base.top + from.height / 2;

  topology.querySelectorAll(".node:not(.is-leader) .node__disc").forEach((disc, i) => {
    const to = disc.getBoundingClientRect();
    const tx = to.left - base.left + to.width / 2;
    const ty = to.top - base.top + to.height / 2;

    const packet = document.createElement("span");
    packet.className = "packet";
    packet.style.left = `${sx}px`;
    packet.style.top = `${sy}px`;
    topology.append(packet);

    const anim = packet.animate(
      [
        { transform: "translate(0,0) scale(0.5)", opacity: 0 },
        { transform: "translate(0,0) scale(1)", opacity: 1, offset: 0.18 },
        { transform: `translate(${tx - sx}px, ${ty - sy}px) scale(1)`, opacity: 1, offset: 0.82 },
        { transform: `translate(${tx - sx}px, ${ty - sy}px) scale(0.5)`, opacity: 0 },
      ],
      { duration: 620, delay: i * 55, easing: "cubic-bezier(0.4, 0, 0.2, 1)" }
    );
    anim.onfinish = () => packet.remove();

    const follower = disc.parentElement;
    setTimeout(() => {
      follower.classList.add("relay");
      setTimeout(() => follower.classList.remove("relay"), 300);
    }, i * 55 + 500);
  });
}

function renderSpark() {
  if (latencies.length < 2) {
    spark.replaceChildren();
    return;
  }
  const max = Math.max(...latencies, 1);
  const n = latencies.length;
  const points = latencies
    .map((v, i) => `${((i / (n - 1)) * 100).toFixed(1)},${(21 - (v / max) * 19).toFixed(1)}`)
    .join(" ");
  const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  line.setAttribute("points", points);
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", "currentColor");
  line.setAttribute("stroke-width", "1.2");
  line.setAttribute("vector-effect", "non-scaling-stroke");
  spark.replaceChildren(line);
}

function setOp(op) {
  activeOp = op;
  segSlide.style.transform = `translateX(${INDEX[op] * 100}%)`;
  document.querySelectorAll(".seg__btn").forEach((b) => b.classList.toggle("is-active", b.dataset.op === op));
  valueField.classList.toggle("is-hidden", op !== "put");
  io.classList.toggle("is-single", op !== "put");
  modeLabel.textContent = op;
  executeLabel.textContent = LABELS[op];
}

function setStatus(state, text) {
  statusEl.className = `status ${state}`;
  statusEl.textContent = text;
}

function rememberKey(key) {
  if (!key) return;
  recent.delete(key);
  recent.add(key);
  const keys = [...recent].reverse().slice(0, 12);
  recentKeys.replaceChildren(
    ...keys.map((k) => {
      const opt = document.createElement("option");
      opt.value = k;
      return opt;
    })
  );
}

function flash() {
  resultBox.classList.remove("flash");
  void resultBox.offsetWidth;
  resultBox.classList.add("flash");
}

function pushLog(op, detail, ms, ok) {
  const empty = log.querySelector(".log__empty");
  if (empty) empty.remove();

  const li = document.createElement("li");

  const time = document.createElement("span");
  time.className = "log__time";
  time.textContent = new Date().toLocaleTimeString("en-GB");

  const tag = document.createElement("span");
  tag.className = `log__op log__op--${ok ? op : "err"}`;
  tag.textContent = ok ? op : "err";

  const det = document.createElement("span");
  det.className = "log__detail";
  det.textContent = detail;

  const lat = document.createElement("span");
  lat.className = "log__lat";
  lat.textContent = ms == null ? "" : `${ms} ms`;

  li.append(time, tag, det, lat);
  log.prepend(li);
  while (log.children.length > 18) log.lastElementChild.remove();
}

async function callApi(op, body) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(`/api/${op}`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
      signal: controller.signal,
    });
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

function payload() {
  const body = new URLSearchParams();
  body.set("nodes", nodeList().join(","));
  body.set("key", keyInput.value);
  body.set("value", valueInput.value);
  return body;
}

async function run(event) {
  event.preventDefault();
  execute.disabled = true;
  setStatus("is-busy", "working");
  resultValue.className = "";
  resultValue.textContent = "contacting cluster…";

  const op = activeOp;
  const key = keyInput.value;
  const value = valueInput.value;

  try {
    const data = await callApi(op, payload());
    topology.classList.remove("is-offline");

    if (!data.ok) {
      setStatus("is-err", "error");
      resultValue.className = "is-err";
      resultValue.textContent = data.error || "request failed";
      latencyEl.textContent = data.elapsedMs ?? "—";
      pushLog(op, data.error || "request failed", data.elapsedMs, false);
      return;
    }

    operations += 1;
    opCount.textContent = operations;
    latencyEl.textContent = data.elapsedMs;
    latencies.push(data.elapsedMs);
    if (latencies.length > 24) latencies.shift();
    renderSpark();
    if (typeof data.leader === "number") setLeader(data.leader);
    setStatus("is-ok", "ok");
    leaderPulse();
    if (op !== "get") emitPackets();
    rememberKey(key);
    flash();

    let detail;
    if (op === "get") {
      if (data.value === null) {
        resultValue.className = "is-nil";
        resultValue.textContent = "(nil)";
        detail = `${key} → (nil)`;
      } else {
        resultValue.className = "";
        resultValue.textContent = data.value;
        detail = `${key} → ${data.value}`;
      }
    } else if (op === "put") {
      resultValue.className = "";
      resultValue.textContent = value || "(empty)";
      detail = `${key} = ${value}`;
    } else {
      resultValue.className = "is-nil";
      resultValue.textContent = "(deleted)";
      detail = key;
    }
    pushLog(op, detail, data.elapsedMs, true);
  } catch (error) {
    const offline = error.name === "AbortError";
    setStatus("is-err", offline ? "offline" : "error");
    healthDot.classList.remove("is-live");
    healthDot.classList.add("is-offline");
    topology.classList.add("is-offline");
    resultValue.className = "is-err";
    resultValue.textContent = offline ? "cluster unreachable — is a node running?" : error.message;
    latencyEl.textContent = "—";
    pushLog(op, offline ? "cluster unreachable" : error.message, null, false);
  } finally {
    execute.disabled = false;
  }
}

document.querySelectorAll(".seg__btn").forEach((b) => b.addEventListener("click", () => setOp(b.dataset.op)));
$("#reset-nodes").addEventListener("click", () => {
  nodesInput.value = DEFAULT_NODES;
  renderTopology();
});
$("#clear-log").addEventListener("click", () => {
  log.innerHTML = '<li class="log__empty">no operations yet</li>';
});
nodesInput.addEventListener("input", renderTopology);
form.addEventListener("submit", run);

document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault();
    form.requestSubmit();
    return;
  }
  if (["INPUT", "TEXTAREA"].includes(document.activeElement.tagName)) return;
  if (e.key === "1") setOp("put");
  else if (e.key === "2") setOp("get");
  else if (e.key === "3") setOp("delete");
  else if (e.key === "/") {
    e.preventDefault();
    keyInput.focus();
  }
});

setOp("put");
renderTopology();
