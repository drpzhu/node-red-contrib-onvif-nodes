/**
 * onvif_events.js — PullPoint polling with gentler pacing + auto-start
 * Works with onvif@0.6.x (subscription.pullMessages OR cam.pullMessages).
 *
 * Features:
 *  - Auto-start on deploy/reconnect (no Inject needed)
 *  - Auto-retry every N ms until camera is ready
 *  - PullPoint polling with small idle delays (reduces duplicates)
 *  - SetSynchronizationPoint (if available)
 *  - Auto-renew subscription (if available)
 *  - State-change + TTL de-dup for IsMotion
 */

module.exports = function (RED) {
  function OnvifEventsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // ---- Config / options (add to .html later if you want GUI toggles) ----
    node.deviceConfig = RED.nodes.getNode(config.config);
    node.subscriptionMode = (config.subscriptionMode || "pull"); // "pull" default here
    node.pullTimeout = config.pullTimeout || "PT1S";
    node.pullLimit = Number(config.pullLimit || 100);
    node.autoStart = config.autoStart !== false; // default true
    node.autoRetryMs = Number(config.autoRetryMs || 10000); // retry every 10s by default
    node.gentleDelayWithMsgs = Number(config.gentleDelayWithMsgs || 50);  // ms after a batch
    node.gentleDelayNoMsgs   = Number(config.gentleDelayNoMsgs   || 250); // ms when idle
    node.enableSyncPoint = config.syncPoint !== false; // call SetSynchronizationPoint once
    node.enableAutoRenew = config.renewAuto !== false; // auto-renew if supported

    // ---- Internal state ----
    let started = false;
    let autoRetryTimer = null;
    node.stopPulling = false;
    node.subscription = null;
    node.renewalTimer = null;
    node.errorCount = 0;

    // De-dup helpers
    node._lastState = new Map(); // topic => 'true'|'false'
    node._seenTTL = new Map();   // hash => timestamp(ms)

    // ---- Helpers ----
    function pruneTTL(now) {
      for (const [k, t] of node._seenTTL) {
        if (now - t > 10_000) node._seenTTL.delete(k); // 10s TTL
      }
    }

    function deNs(topic) {
      if (typeof topic !== "string") return topic;
      return topic.split("/").map(p => p.split(":").pop()).join("/");
    }

    function clearRenew() {
      if (node.renewalTimer) {
        clearInterval(node.renewalTimer);
        node.renewalTimer = null;
      }
    }

    function stopListening() {
      node.stopPulling = true;
      started = false;
      clearRenew();
      try {
        if (node.subscription && typeof node.subscription.unsubscribe === "function") {
          node.subscription.unsubscribe(() => {});
        }
      } catch (e) {
        // ignore
      }
      node.subscription = null;
      node.status({ fill: "grey", shape: "ring", text: "stopped" });
    }

    // ---- Event message normalizer + de-dup ----
    node.processEventMessage = function (camMessage) {
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

      // Source (first simpleItem)
      if (mm.source && mm.source.simpleItem) {
        const s = Array.isArray(mm.source.simpleItem)
          ? mm.source.simpleItem[0]
          : mm.source.simpleItem;
        if (s && s.$) out.source = { name: s.$.Name, value: s.$.Value };
      }
      if (mm.key) out.key = mm.key;

      // Data
      if (mm.data && mm.data.simpleItem) {
        if (Array.isArray(mm.data.simpleItem)) {
          out.data = mm.data.simpleItem.map(x => x && x.$ ? ({ name: x.$.Name, value: x.$.Value }) : x);
        } else if (mm.data.simpleItem.$) {
          out.data = { name: mm.data.simpleItem.$.Name, value: mm.data.simpleItem.$.Value };
        }
      } else if (mm.data && mm.data.elementItem) {
        out.data = { dataName: "elementItem", dataValue: JSON.stringify(mm.data.elementItem) };
      }

      // Ignore initial "Initialized"
      if (out.property && /initialized/i.test(out.property)) return;

      // Extract IsMotion (as 'true'|'false' if present)
      let isMotion;
      if (out.data) {
        if (Array.isArray(out.data)) {
          const f = out.data.find(x => x && x.name === "IsMotion");
          if (f) isMotion = String(f.value).toLowerCase();
        } else if (out.data.name === "IsMotion") {
          isMotion = String(out.data.value).toLowerCase();
        }
      }

      // State-change filter per topic
      const keyTopic = out.topic || "unknown";
      if (isMotion === "true" || isMotion === "false") {
        const prev = node._lastState.get(keyTopic);
        if (prev === isMotion) return; // drop identical state repeats
        node._lastState.set(keyTopic, isMotion);
      }

      // TTL de-dup for identical payloads within 10s
      const hash = keyTopic + "|" + (out.time || "") + "|" + JSON.stringify(out.data || {});
      const now = Date.now();
      if (node._seenTTL.has(hash)) {
        if (now - node._seenTTL.get(hash) < 10_000) return; // drop duplicate
      }
      node._seenTTL.set(hash, now);
      pruneTTL(now);

      node.send(out);
    };

    // ---- Start (PullPoint) ----
    function startListening() {
    if (started) return; // idempotent


    // Camera instance not ready yet? show status and retry in 1s
    // 🔧 Re-fetch the config node each attempt (it may not have been ready earlier)
    node.deviceConfig = RED.nodes.getNode(config.config);

    if (!node.deviceConfig || !node.deviceConfig.cam) {
        node.status({ fill: "yellow", shape: "ring", text: "waiting for camera" });
        // (optional) throttle this log if it's too chatty
        node._lastWaitLog = node._lastWaitLog || 0;
        const now = Date.now();
        if (now - node._lastWaitLog > 5000) {
        node.warn("onvif-events: deviceConfig=" + !!node.deviceConfig + " cam=" + (node.deviceConfig && !!node.deviceConfig.cam));
        node._lastWaitLog = now;
        }
        // try again shortly; don't rely only on the 10s global loop
        setTimeout(() => { if (!started) startListening(); }, 1000);
        return;
    }

    // This build only implements PullPoint
    if (node.subscriptionMode !== "pull") {
        node.status({ fill: "red", shape: "ring", text: "only pull mode supported in this build" });
        return;
    }

    started = true;
    node.stopPulling = false;

    // Create PullPoint subscription
    node.deviceConfig.cam.createPullPointSubscription(function (err, subscription /*, terminationTime */) {
        if (err) {
        started = false;
        node.status({ fill: "red", shape: "ring", text: ("pullpoint failed: " + String(err)).slice(0, 60) });
        return;
        }

        node.subscription = subscription;
        node.errorCount = 0;

        // Pick a working pullMessages (subscription or cam) for onvif@0.6.x
        const pullFn =
        (subscription && typeof subscription.pullMessages === "function" && subscription.pullMessages.bind(subscription)) ||
        (subscription && typeof subscription.PullMessages === "function" && subscription.PullMessages.bind(subscription)) ||
        (node.deviceConfig.cam && typeof node.deviceConfig.cam.pullMessages === "function" && node.deviceConfig.cam.pullMessages.bind(node.deviceConfig.cam));

        if (!pullFn) {
        started = false;
        node.error("No pullMessages on subscription/cam (check onvif version).");
        return;
        }

        // Optional sync point (on sub or cam)
        if (node.enableSyncPoint) {
        const setSync =
            (subscription && typeof subscription.setSynchronizationPoint === "function" && subscription.setSynchronizationPoint.bind(subscription)) ||
            (node.deviceConfig.cam && typeof node.deviceConfig.cam.setSynchronizationPoint === "function" && node.deviceConfig.cam.setSynchronizationPoint.bind(node.deviceConfig.cam));
        try { setSync && setSync(() => {}); } catch (e) {}
        }

        node.status({ fill: "green", shape: "dot", text: "listening (pull)" });

        // Auto-renew (if supported)
        if (node.enableAutoRenew && subscription && typeof subscription.renew === "function") {
        node.renewalTimer = setInterval(() => {
            if (!node.stopPulling) { try { subscription.renew(() => {}); } catch (e) {} }
        }, 60_000);
        }

        // Poll loop with gentle pacing + exponential backoff on error
        function poll() {
        if (node.stopPulling) return;

        pullFn({ timeout: node.pullTimeout, messageLimit: node.pullLimit }, function (err, res) {
            if (err) {
            node.errorCount = Math.min(node.errorCount + 1, 10);
            const backoff = Math.min(1000 * Math.pow(2, node.errorCount - 1), 10000);
            return void setTimeout(poll, backoff);
            }
            node.errorCount = 0;

            const list = res && res.notificationMessage
            ? (Array.isArray(res.notificationMessage) ? res.notificationMessage : [res.notificationMessage])
            : [];

            for (const n of list) {
            const camMessage = { topic: n.topic || n.Topic, message: n.message || n.Message };
            node.processEventMessage && node.processEventMessage(camMessage);
            }

            const delay = list.length ? node.gentleDelayWithMsgs : node.gentleDelayNoMsgs; // e.g. 50/250ms
            setTimeout(poll, delay);
        });
        }

        poll();
    });
    }

    // ---- Input API (backward compatible) ----
    node.on("input", (msg) => {
      const action = (msg && msg.action) || (msg && msg.payload && msg.payload.action);
      switch (action) {
        case "start":
          startListening();
          break;
        case "stop":
          stopListening();
          break;

        case "getEventProperties":
          if (!node.deviceConfig || !node.deviceConfig.cam) return;
          node.deviceConfig.cam.getEventProperties(function (err, tree /*, xml */) {
            if (err) return node.error("getEventProperties failed: " + err);
            // Minimal passthrough (you can simplify further if you used utils previously)
            node.send({ topic: "getEventProperties", payload: tree });
          });
          break;

        case "getEventServiceCapabilities":
          if (!node.deviceConfig || !node.deviceConfig.cam) return;
          node.deviceConfig.cam.getEventServiceCapabilities(function (err, caps /*, xml */) {
            if (err) return node.error("getEventServiceCapabilities failed: " + err);
            node.send({ topic: "getEventServiceCapabilities", payload: caps });
          });
          break;

        default:
          // ignore
          break;
      }
    });

    // ---- Auto-start + auto-retry (no Inject node needed) ----
    if (node.autoStart) {
        setTimeout(() => startListening(), 500);
        autoRetryTimer = setInterval(() => {
            // 🔧 refresh the config node before attempting again
            node.deviceConfig = RED.nodes.getNode(config.config);
            if (!started) startListening();
        }, node.autoRetryMs);
    }

    // ---- Clean up ----
    node.on("close", (removed, done) => {
      if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
      stopListening();
      done && done();
    });

    // Initial status
    node.status({ fill: "grey", shape: "ring", text: "idle" });
  }

  RED.nodes.registerType("onvif-events", OnvifEventsNode);
};
