// orchestra service worker: shows pushes, answers "take default" from the notification.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("push", (event) => {
  let p = {};
  try { p = event.data ? event.data.json() : {}; } catch { p = { title: "orchestra", body: event.data && event.data.text() }; }
  event.waitUntil(self.registration.showNotification(p.title || "orchestra", {
    body: p.body || "",
    tag: p.tag,
    renotify: true,
    icon: "/icon.svg",
    badge: "/icon.svg",
    requireInteraction: !!p.requireInteraction,
    actions: p.actions || [],
    data: p,
  }));
});

async function focusOrOpen(url) {
  const all = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  for (const c of all) {
    if (new URL(c.url).origin === self.location.origin) {
      await c.focus();
      if ("navigate" in c) return c.navigate(url);
      return;
    }
  }
  return self.clients.openWindow(url);
}

self.addEventListener("notificationclick", (event) => {
  const p = event.notification.data || {};
  event.notification.close();
  if (event.action === "default" && p.thread) {
    event.waitUntil((async () => {
      const r = await fetch("/api/events", {
        method: "POST", credentials: "same-origin", headers: { "content-type": "application/json" },
        body: JSON.stringify(p.options
          ? { from: "operator", kind: "answer", ref: p.thread, body: "took the default (from the notification)", choice: [p.default] }
          : { from: "operator", kind: "answer", ref: p.thread, body: `take the default: ${p.default || ""}` }),
      });
      const ok = r.ok || r.status === 409;
      await self.registration.showNotification(ok ? "default taken ✓" : "couldn't answer — open the inbox", {
        body: ok ? `${p.thread}` : `HTTP ${r.status}`, tag: "ack-" + p.thread, icon: "/icon.svg",
        data: { url: p.url || "/" },
      });
    })());
    return;
  }
  event.waitUntil(focusOrOpen(p.url || "/"));
});
