// 按批次复核：选定候选 → 写理由 → 保存裁决 → 重建复核区间。
// 每条裁决带当前版本号；服务端以版本号拒绝过期决定（409 E_STALE_DECISION）。
const panel = document.getElementById("review-panel");

function notice(message, isError = false) {
  const box = document.getElementById("notice");
  if (!box) return;
  box.textContent = message;
  box.classList.toggle("is-error", Boolean(isError));
  box.hidden = false;
}

async function sendJSON(method, url, body) {
  const response = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch (error) {
    payload = { error: "E_BAD_RESPONSE", message: "响应不是 JSON" };
  }
  if (!response.ok) {
    const code = payload && payload.error ? payload.error : response.status;
    const message = payload && payload.message ? payload.message : "请求失败";
    const revision = payload && payload.detail && payload.detail.current_revision;
    const suffix = revision !== undefined ? "（当前版本 " + revision + "）" : "";
    const error = new Error(code + " " + message + suffix);
    error.code = code;
    error.payload = payload;
    throw error;
  }
  return payload;
}

function rowOf(button) {
  return button.closest("tr[data-group]");
}

async function decide(button) {
  const row = rowOf(button);
  const batch = panel.dataset.batch;
  const groupKey = row.dataset.group;
  const picked = row.querySelector('input[type="radio"]:checked');
  if (!picked) {
    notice("先选定一条候选读数", true);
    return;
  }
  const reason = row.querySelector(".input-reason").value.trim();
  if (!reason) {
    notice("裁决必须写明理由", true);
    row.querySelector(".input-reason").focus();
    return;
  }
  button.disabled = true;
  try {
    await sendJSON("POST", "/api/batches/" + encodeURIComponent(batch) + "/review/decisions", {
      group_key: groupKey,
      selected_lineno: Number(picked.value),
      reason,
      expected_revision: Number(row.dataset.revision || 0),
    });
    notice("裁决已保存：行 " + picked.value);
    window.setTimeout(() => window.location.reload(), 350);
  } catch (error) {
    notice(String(error.message || error), true);
    if (error.code === "E_STALE_DECISION") {
      window.setTimeout(() => window.location.reload(), 1200);
    }
    button.disabled = false;
  }
}

async function withdraw(button) {
  const row = rowOf(button);
  const batch = panel.dataset.batch;
  const groupKey = row.dataset.group;
  const reason = row.querySelector(".input-reason").value.trim() || "人工撤回";
  button.disabled = true;
  try {
    await sendJSON("DELETE", "/api/batches/" + encodeURIComponent(batch) +
      "/review/decisions/" + encodeURIComponent(groupKey),
      { reason, expected_revision: Number(row.dataset.revision || 0) });
    notice("已撤回该组裁决，原隔离结论仍在");
    window.setTimeout(() => window.location.reload(), 350);
  } catch (error) {
    notice(String(error.message || error), true);
    if (error.code === "E_STALE_DECISION") {
      window.setTimeout(() => window.location.reload(), 1200);
    }
    button.disabled = false;
  }
}

async function rebuild(button) {
  const batch = panel.dataset.batch;
  button.disabled = true;
  const label = button.textContent;
  button.textContent = "重建中";
  try {
    const payload = await sendJSON("POST",
      "/api/batches/" + encodeURIComponent(batch) + "/review/rebuild", {});
    notice("复核区间已重建 · 用量 " + payload.rebuilt.usage_kwh + " kWh");
    window.setTimeout(() => window.location.reload(), 400);
  } catch (error) {
    notice(String(error.message || error), true);
  } finally {
    button.textContent = label;
    button.disabled = false;
  }
}

if (panel) {
  panel.querySelectorAll(".review-decide").forEach((button) => {
    button.addEventListener("click", () => decide(button));
  });
  panel.querySelectorAll(".review-withdraw").forEach((button) => {
    button.addEventListener("click", () => withdraw(button));
  });
  const rebuildButton = panel.querySelector("#review-rebuild-button");
  if (rebuildButton) {
    rebuildButton.addEventListener("click", () => rebuild(rebuildButton));
  }
  panel.querySelectorAll('input[type="radio"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      const row = radio.closest("tr[data-group]");
      row.querySelectorAll(".candidate").forEach((label) => {
        label.classList.toggle("is-selected", label.contains(radio) && radio.checked);
      });
    });
  });
}
