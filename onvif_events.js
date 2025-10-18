/**
 * Copyright 2018 Bart Butenaers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
 // onvif_events.js — PullPoint (Inject-to-start) with auto-renew + watchdog + gentle pacing

module.exports = function (RED) {
  const onvif = require("onvif");
  const { URL } = require("url");

  function OnvifEventsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // ---- Camera config node (selected in the editor) ----
    node.deviceConfig = RED.nodes.getNode(config.config);

    // ---- Sensible defaults (so missing editor fields won't break us) ----
    node.pullTimeout = node.pullTimeout || "PT1S";
    node.pullLimit = Number(node.pullLimit || 100);
    node.gentleDelayWithMsgs = Number(node.gentleDelayWithMsgs || 50);   // ms after a batch
    node.gentleDelayNoMsgs   = Number(node.gentleDelayNoMsgs   || 250);  // ms when idle
    node.enableAutoRenew = (typeof node.enableAutoRenew === "boolean") ? node.enableAutoRenew : true;

    // ---- Internal state ----
    node.stopPulling = false;
    node.subscription = null;
    node.renewalTimer = null;
    node.errorCount = 0;

    // ---- De-dup helpers ----
    node._lastState = new Map(); // topic => 'true'|'false'
    node._seenTTL = new Map();   // hash => timestamp

    function pruneTTL(nowMs) {
      for (const [k, t] of node._seenTTL) {
        if (nowMs - t > 10000) node._seenTTL.delete(k); // 10s TTL window
      }
    }

    function deNs(topic) {
      if (typeof topic !== "string") return topic;
      return topic.split("/").map(p => p.split(":").pop()).join("/");
    }

    // ---- (Optional) build a local Cam if config.cam isn’t ready ----
    async function makeLocalCamFromConfig(cfgNode) {
    if (!cfgNode) return null;

    // Prefer xaddress, but tolerate bare host:port or just host
    let raw = (cfgNode.xaddress || "").trim();
    if (!raw) {
        // try separate fields some configs use
        const host = (cfgNode.host || cfgNode.hostname || "").trim();
        const port = (cfgNode.port || "").toString().trim();
        if (!host) return null;
        raw = host + (port ? (":" + port) : "");
    }

    // Add scheme if missing
    if (!/^https?:\/\//i.test(raw)) raw = "http://" + raw;

    // Ensure path
    if (/^https?:\/\/[^/]+\/?$/i.test(raw)) {
        // no path -> default ONVIF device service path
        if (!raw.endsWith("/")) raw += "/";
        raw += "onvif/device_service";
    }

    let u;
    try {
        u = new URL(raw);
    } catch (e) {
        node.warn("makeLocalCamFromConfig error (normalize): " + e.message + " | raw=" + raw);
        return null;
    }

    // Default port: if none given, use 2020 (Tapo); otherwise use provided
    const portNum = u.port ? Number(u.port) : 2020;

    const opts = {
        hostname: u.hostname,
        port: portNum,
        username: cfgNode.user || cfgNode.username,
        password: cfgNode.pass || cfgNode.password,
        path: u.pathname || "/onvif/device_service",
        timeout: 5000
    };

    // Breadcrumb to confirm what we’re dialing (password omitted)
    node.status({ fill: "yellow", shape: "ring", text: `connecting ${opts.hostname}:${opts.port}${opts.path}` });

    return await new Promise((resolve, reject) => {
        // eslint-disable-next-line no-new
        new (require("onvif").Cam)(opts, function (err) {
        if (err) return reject(err);
        resolve(this); // 'this' is the Cam instance
        });
    });
    }

    // ---- Normalize + de-dup outgoing events ----
    function emitEvent(camMessage) {
      if (!camMessage) return;

      const rawTopic = (camMessage.topic && (camMessage.topic._ || camMessage.topic)) || "";
      const eventTopic = deNs(rawTopic);
      const mm = camMessage.message && camMessage.message.message;
      if (!mm || !mm.$) return;

      const out = {
        topic: eventTopic,
        time: mm.$.UtcTime,
        property: mm.$.PropertyOperation
      };

      // source (first simpleItem)
      if (mm.source && mm.source.simpleItem) {
        const s = Array.isArray(mm.source.simpleItem)
          ? mm.source.simpleItem[0]
          : mm.source.simpleItem;
        if (s && s.$) out.source = { name: s.$.Name, value: s.$.Value };
      }
      if (mm.key) out.key = mm.key;

      // data
      if (mm.data && mm.data.simpleItem) {
        if (Array.isArray(mm.data.simpleItem)) {
          out.data = mm.data.simpleItem.map(x => x && x.$ ? ({ name: x.$.Name, value: x.$.Value }) : x);
        } else if (mm.data.simpleItem.$) {
          out.data = { name: mm.data.simpleItem.$.Name, value: mm.data.simpleItem.$.Value };
        }
      } else if (mm.data && mm.data.elementItem) {
        out.data = { dataName: "elementItem", dataValue: JSON.stringify(mm.data.elementItem) };
      }

      // Ignore one-time "Initialized"
      if (out.property && /initialized/i.test(out.property)) return;

      // State-change filter for IsMotion / IsPeople etc.
      let stateStr;
      if (out.data) {
        const items = Array.isArray(out.data) ? out.data : [out.data];
        const stateItem = items.find(x => x && (x.name === "IsMotion" || x.name === "IsPeople"));
        if (stateItem) stateStr = String(stateItem.value).toLowerCase();
      }
      const keyTopic = out.topic || "unknown";
      if (stateStr === "true" || stateStr === "false") {
        const prev = node._lastState.get(keyTopic);
        if (prev === stateStr) return;          // drop identical state
        node._lastState.set(keyTopic, stateStr);
      }

      // TTL de-dup (identical payloads within 10s)
      const hash = keyTopic + "|" + (out.time || "") + "|" + JSON.stringify(out.data || {});
      const now = Date.now();
      if (node._seenTTL.has(hash) && (now - node._seenTTL.get(hash) < 10000)) return;
      node._seenTTL.set(hash, now);
      pruneTTL(now);

      // Emit with the event as payload, topic preserved for Node-RED
      node.send({ topic: out.topic, payload: out });
    }

    // ---- Restart helper (used by renew/pull watchdog) ----
    function restartSubscription(reason) {
      node.status({ fill: "yellow", shape: "ring", text: "restarting (" + reason + ")" });
      node.stopPulling = true;

      if (node.renewalTimer) {
        clearInterval(node.renewalTimer);
        node.renewalTimer = null;
      }
      if (node.subscription && typeof node.subscription.unsubscribe === "function") {
        try { node.subscription.unsubscribe(() => {}); } catch (_) {}
      }
      node.subscription = null;

      // Re-enter via our own input path after a short pause
      setTimeout(() => node.receive({ action: "start" }), 1000);
    }

    // ---- Handle incoming actions ----
    node.on("input", async (msg) => {
    const action =
        (msg && msg.action) ||
        (msg && msg.payload && msg.payload.action);

    // ----- STOP -----
    if (action === "stop") {
        node.stopPulling = true;
        if (node.renewalTimer) { clearInterval(node.renewalTimer); node.renewalTimer = null; }
        if (node.subscription && typeof node.subscription.unsubscribe === "function") {
        try { node.subscription.unsubscribe(() => {}); } catch (_) {}
        }
        node.subscription = null;
        node.status({ fill: "grey", shape: "ring", text: "stopped" });
        return;
    }

    // ----- READ-ONLY CALLS -----
    if (action === "getEventProperties" || action === "getEventServiceCapabilities") {
        // (re)resolve config and get a cam (prefer config.cam; else build one)
        const cfgId = (config && (config.config || config.deviceConfig || config.camera || config.cam)) || null;
        node.deviceConfig = cfgId ? RED.nodes.getNode(cfgId) : null;

        const cam = (node.deviceConfig && node.deviceConfig.cam) || (await makeLocalCamFromConfig(node.deviceConfig));
        if (!cam) return node.error(action + ": no camera");
        const call = action === "getEventProperties" ? cam.getEventProperties : cam.getEventServiceCapabilities;
        call.call(cam, (err, result /*, xml */) => {
        if (err) return node.error(action + " failed: " + err);
        node.send({ topic: action, payload: result });
        });
        return;
    }

    // ----- START (PullPoint) -----
    if (action === "start") {
        if (node.subscription) {
        node.error("This node is already listening to device events");
        return;
        }

        // (re)resolve config **now** so UI changes take effect immediately
        const cfgId = (config && (config.config || config.deviceConfig || config.camera || config.cam)) || null;
        node.deviceConfig = cfgId ? RED.nodes.getNode(cfgId) : null;

        node.stopPulling = false;

        // Prefer config.cam; else create our own
        let cam = node.deviceConfig && node.deviceConfig.cam;
        if (!cam && node.deviceConfig) {
        node.status({ fill: "yellow", shape: "ring", text: "connecting (own cam)" });
        try { cam = await makeLocalCamFromConfig(node.deviceConfig); } catch (_) {}
        }
        if (!cam) {
        node.status({ fill: "yellow", shape: "ring", text: node.deviceConfig ? "no camera ready" : "no camera configured" });
        return;
        }

        cam.createPullPointSubscription((err, subscription /*, terminationTime */) => {
        if (err) {
            node.status({ fill: "red", shape: "ring", text: "pullpoint failed: " + String(err).slice(0, 60) });
            return;
        }

        node.subscription = subscription;
        node.errorCount = 0;

        const pullFn =
            (subscription && typeof subscription.pullMessages === "function" && subscription.pullMessages.bind(subscription)) ||
            (subscription && typeof subscription.PullMessages === "function" && subscription.PullMessages.bind(subscription)) ||
            (typeof cam.pullMessages === "function" && cam.pullMessages.bind(cam));

        if (!pullFn) {
            node.error("No pullMessages on subscription/cam (check onvif version).");
            return;
        }

        // optional sync point
        const setSync =
            (subscription && typeof subscription.setSynchronizationPoint === "function" && subscription.setSynchronizationPoint.bind(subscription)) ||
            (typeof cam.setSynchronizationPoint === "function" && cam.setSynchronizationPoint.bind(cam));
        try { setSync && setSync(() => {}); } catch (_) {}

        node.status({ fill: "green", shape: "dot", text: "listening (pull)" });
        node.warn("PullPoint ready — using " + (pullFn === (cam && cam.pullMessages) ? "cam.pullMessages" : "subscription.pullMessages"));

        // --- TTL-aware renew (fallback 120s) ---
        function parseIsoToMs(s) { try { return Math.max(0, Date.parse(s) - Date.now()); } catch { return 0; } }
        let ttlMs =
            parseIsoToMs(subscription && (subscription.terminationTime || subscription.TerminationTime));
        if (!ttlMs || !isFinite(ttlMs)) ttlMs = 120000;
        const renewEvery = Math.min(90000, Math.max(30000, Math.floor(ttlMs * 0.6)));
        if (node.enableAutoRenew && typeof subscription.renew === "function") {
            if (node.renewalTimer) { clearInterval(node.renewalTimer); node.renewalTimer = null; }
            node.renewalTimer = setInterval(() => {
            if (node.stopPulling) return;
            subscription.renew((rErr) => {
                if (rErr) {
                node.warn("PullPoint renew failed: " + rErr);
                // self-heal
                restartSubscription("renew-failed");
                }
            });
            }, renewEvery);
        }

        // --- poll with gentle pacing + backoff + watchdog ---
        let lastOk = Date.now();
        const watchdogMs = 75 * 1000;

        function poll() {
            if (node.stopPulling) return;

            pullFn({ timeout: node.pullTimeout, messageLimit: node.pullLimit }, (pErr, res) => {
            if (pErr) {
                node.errorCount = Math.min((node.errorCount || 0) + 1, 10);
                const s = String(pErr || "");
                if (s.includes("wsa:MessageInformationHeaderRequired")
                || s.toLowerCase().includes("terminat")
                || s.toLowerCase().includes("expired")
                || node.errorCount >= 3) {
                return restartSubscription("pull-error");
                }
                const backoff = Math.min(1000 * Math.pow(2, node.errorCount - 1), 10000);
                return void setTimeout(poll, backoff);
            }

            node.errorCount = 0;
            lastOk = Date.now();

            const list = res && res.notificationMessage
                ? (Array.isArray(res.notificationMessage) ? res.notificationMessage : [res.notificationMessage])
                : [];

            for (const n of list) {
                const camMessage = { topic: n.topic || n.Topic, message: n.message || n.Message };
                emitEvent(camMessage);
            }

            const delay = list.length ? node.gentleDelayWithMsgs : node.gentleDelayNoMsgs;
            setTimeout(() => {
                if (Date.now() - lastOk > watchdogMs) return restartSubscription("watchdog");
                poll();
            }, delay);
            });
        }

        poll();
        });

        return; // done with "start"
    }

    // ignore anything else
    });


    // ---- Cleanup on node close ----
    node.on("close", (removed, done) => {
      node.stopPulling = true;
      if (node.renewalTimer) { clearInterval(node.renewalTimer); node.renewalTimer = null; }
      if (node.subscription && typeof node.subscription.unsubscribe === "function") {
        try { node.subscription.unsubscribe(() => {}); } catch (_) {}
      }
      node.subscription = null;
      done && done();
    });

    // initial status
    node.status({ fill: "grey", shape: "ring", text: "idle" });
  }

  RED.nodes.registerType("onvif-events", OnvifEventsNode);
};
