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
 module.exports = function(RED) {
    var settings = RED.settings;
    const onvif = require('onvif');
    const utils = require('./utils');
    
    function OnVifEventsNode(config) {
        RED.nodes.createNode(this, config);
        this.action = config.action;

        var node = this;
        
        // Retrieve the config node, where the device is configured
        node.deviceConfig = RED.nodes.getNode(config.deviceConfig);
        
        if (node.deviceConfig) {
            node.listener = function(onvifStatus) {
                utils.setNodeStatus(node, 'event', onvifStatus);
                
                if (onvifStatus !== "connected" && node.subscription) {
                    // When the device isn't connected anymore, stop pulling events from the camera
                    node.stopPulling = true;
                    
                    // Clear renewal timer
                    if (node.renewalTimer) {
                        clearInterval(node.renewalTimer);
                        node.renewalTimer = null;
                    }
                    
                    if (node.subscription && node.subscription.unsubscribe) {
                        node.subscription.unsubscribe(function(err) {
                            if (err) {
                                console.log("Error unsubscribing on disconnect: " + err);
                            }
                        });
                    }
                    node.subscription = null;
                    node.processEventMessage = null;
                }
            }
            
            // Start listening for Onvif config nodes status changes
            node.deviceConfig.addListener("onvif_status", node.listener);
            
            // Show the current Onvif config node status already
            utils.setNodeStatus(node, 'event', node.deviceConfig.onvifStatus);
            
            node.deviceConfig.initialize();
        }
               
        node.on("input", function(msg) {  
            var newMsg = {};
            
            // Note: the node's config screen has no 'action' input field yet ...
            var action = node.action || msg.action;
            
            if (!action) {
                // When no action specified in the node, it should be specified in the msg.action
                node.error("No action specified (in node or msg)");
                return;
            }
            
            // Don't perform these checks when e.g. the device is currently disconnected (because then e.g. no capabilities are loaded yet)
            if (action !== "reconnect") {
                if (!node.deviceConfig || node.deviceConfig.onvifStatus != "connected") {
                    node.error("This node is not connected to a device");
                    return;
                }

                if (!utils.hasService(node.deviceConfig.cam, 'event')) {
                    node.error("The device has no support for an event service");
                    return;
                }
            }
            
            // Seems that some Axis cams support pull point, although they return WSPullPointSupport 'false'
            /*if (!node.deviceConfig.cam.capabilities.events.WSPullPointSupport == true) {
                //console.warn('Ignoring input message since the device does not support pull point subscription');
                return;
            }*/
            
            newMsg.xaddr = this.deviceConfig.xaddress;
            newMsg.action = action;

            try {
                switch (action) {
                    case "start":
                        if (node.subscription) {
                            node.error("This node is already listening to device events");
                            return;
                        }

                        // define processor BEFORE any polling can happen
                        node.processEventMessage = function (camMessage) {
                            try {
                            if (!camMessage) return;

                            const topicRaw = (camMessage.topic && (camMessage.topic._ || camMessage.topic)) || "";
                            const eventTopic = (typeof topicRaw === "string")
                                ? topicRaw.split("/").map(p => p.split(":").pop()).join("/")
                                : topicRaw;

                            const mm = camMessage.message && camMessage.message.message;
                            if (!mm || !mm.$) return;

                            const out = {
                                topic: eventTopic,
                                time: mm.$.UtcTime,
                                property: mm.$.PropertyOperation
                            };

                            if (mm.source && mm.source.simpleItem) {
                                const s = Array.isArray(mm.source.simpleItem) ? mm.source.simpleItem[0] : mm.source.simpleItem;
                                if (s && s.$) out.source = { name: s.$.Name, value: s.$.Value };
                            }
                            if (mm.key) out.key = mm.key;

                            if (mm.data && mm.data.simpleItem) {
                                if (Array.isArray(mm.data.simpleItem)) {
                                out.data = mm.data.simpleItem.map(x => x.$ ? ({ name: x.$.Name, value: x.$.Value }) : x);
                                } else if (mm.data.simpleItem.$) {
                                out.data = { name: mm.data.simpleItem.$.Name, value: mm.data.simpleItem.$.Value };
                                }
                            } else if (mm.data && mm.data.elementItem) {
                                out.data = { dataName: "elementItem", dataValue: JSON.stringify(mm.data.elementItem) };
                            }

							node.send({ topic: out.topic, payload: out });

                            } catch (e) {
                            node.warn("processEventMessage error: " + e);
                            }
                        };

                        // create the PullPoint subscription
                        node.deviceConfig.cam.createPullPointSubscription(function (err, subscription) {
                            if (err) {
                            node.error("Failed to create pull point subscription: " + err);
                            return;
                            }

                            node.subscription = subscription;
                            node.stopPulling = false;
                            node.errorCount = 0;

                            // pick a working pull function (sub vs cam) for onvif@0.6.9 compatibility
                            const pullFn =
                            (subscription && typeof subscription.pullMessages === "function" && subscription.pullMessages.bind(subscription)) ||
                            (subscription && typeof subscription.PullMessages === "function" && subscription.PullMessages.bind(subscription)) ||
                            (node.deviceConfig.cam && typeof node.deviceConfig.cam.pullMessages === "function" && node.deviceConfig.cam.pullMessages.bind(node.deviceConfig.cam));

                            if (!pullFn) {
                            node.error("PullPoint has no pullMessages/PullMessages and cam has no pullMessages. Check onvif version.");
                            return;
                            }

                            // Set sync point if available (on sub or cam, varies by lib)
                            const setSync =
                            (subscription && typeof subscription.setSynchronizationPoint === "function" && subscription.setSynchronizationPoint.bind(subscription)) ||
                            (node.deviceConfig.cam && typeof node.deviceConfig.cam.setSynchronizationPoint === "function" && node.deviceConfig.cam.setSynchronizationPoint.bind(node.deviceConfig.cam));

                            if (setSync) {
                            try { setSync(() => {}); } catch (e) { /* ignore */ }
                            }

                            node.status({ fill: "green", shape: "dot", text: "listening (pull)" });
                            node.warn("PullPoint ready — using " + (pullFn === node.deviceConfig.cam.pullMessages ? "cam.pullMessages" : "subscription.pullMessages"));

                            // single poll loop with exponential backoff
                            function poll() {
                            if (node.stopPulling) return;

                            pullFn({ timeout: "PT1S", messageLimit: 100 }, function (err, res) {
                                if (err) {
                                node.errorCount = Math.min((node.errorCount || 0) + 1, 10);
                                const backoff = Math.min(1000 * Math.pow(2, node.errorCount - 1), 10000);
                                if (!node.stopPulling) setTimeout(poll, backoff);
                                return;
                                }
                                node.errorCount = 0;

                                const list = res && res.notificationMessage
                                ? (Array.isArray(res.notificationMessage) ? res.notificationMessage : [res.notificationMessage])
                                : [];

                                for (const n of list) {
                                const camMessage = { topic: n.topic || n.Topic, message: n.message || n.Message };
                                if (node.processEventMessage) node.processEventMessage(camMessage);
                                }

                                if (!node.stopPulling) setTimeout(poll, 0);
                            });
                            }

                            // renew timer (some stacks expose renew on sub; fine to skip if absent)
                            if (subscription && typeof subscription.renew === "function") {
                            node.renewalTimer = setInterval(() => {
                                if (!node.stopPulling) subscription.renew(() => {});
                            }, 60000);
                            }

                            poll();
                        });

                        break;
                    case "stop":
                        if (!node.subscription) {
                            node.error("This node was not listening to events anyway");
                            return;
                        }

                        // Stop the polling loop
                        node.stopPulling = true;
                        
                        // Clear renewal timer
                        if (node.renewalTimer) {
                            clearInterval(node.renewalTimer);
                            node.renewalTimer = null;
                        }
                        
                        // Unsubscribe from pull point
                        if (node.subscription && node.subscription.unsubscribe) {
                            node.subscription.unsubscribe(function(err) {
                                if (err) {
                                    console.log("Error unsubscribing: " + err);
                                }
                            });
                        }
                        
                        node.subscription = null;
                        node.processEventMessage = null;
                        
                        // Overwrite the device status text
                        node.status({fill:"green",shape:"ring",text:"not listening"}); 
                        break;               
                    case "getEventProperties":
                        node.deviceConfig.cam.getEventProperties(function(err, eventProperties, xml) {
                            if (!err) {
                                var simplifiedProperties = {};
                                
                                // Simplify the soap message to a compact message, by keeping only all relevant information
                                function simplifyNode(treeNode, simplifiedChild) {
                                    // loop over all the child nodes in this node
                                    for (const child in treeNode) {
                                        switch (child) {
                                            case "$":
                                                // Continue to the next child in the list (same level)
                                                continue;
                                            case "messageDescription":
                                                // Collect the details that belong to the event
                                                if (treeNode[child].source && treeNode[child].source.simpleItemDescription) {
                                                    simplifiedChild.source = treeNode[child].source.simpleItemDescription.$;
                                                }
                                                if (treeNode[child].data && treeNode[child].data.simpleItemDescription) {
                                                    simplifiedChild.data = treeNode[child].data.simpleItemDescription.$;
                                                }
                                                
                                                return;
                                            default:
                                                // Descend recursively into the child node, looking for the messageDescription
                                                simplifiedChild[child] = {};
                                                simplifyNode(treeNode[child], simplifiedChild[child]);
                                        }
                                    }
                                }
                                
                                if (eventProperties && eventProperties.topicSet) {
                                    simplifyNode(eventProperties.topicSet, simplifiedProperties);
                                }
                            }
                            
                            utils.handleResult(node, err, simplifiedProperties, null, newMsg);
                        });
                        break;
                    case "getEventServiceCapabilities":
                        node.deviceConfig.cam.getEventServiceCapabilities(function(err, capabilities, xml) {
                            utils.handleResult(node, err, capabilities, xml, newMsg);
                        });
                        break;
                    case "reconnect":
                        node.deviceConfig.cam.connect(function(err) {
                            utils.handleResult(node, err, "", null, newMsg);
                        });
                        break
                    default:
                        //node.status({fill:"red",shape:"dot",text: "unsupported action"});
                        node.error("Action " + action + " is not supported");                   
                }
            }
            catch (exc) {
                node.error("Action " + action + " failed: " + exc);
            }
        });
        
        node.on("close",function() { 
            if (node.listener) {
                node.deviceConfig.removeListener("onvif_status", node.listener);
            }
            
            // Stop the polling loop
            node.stopPulling = true;
            
            // Clear renewal timer
            if (node.renewalTimer) {
                clearInterval(node.renewalTimer);
                node.renewalTimer = null;
            }
            
            // Unsubscribe from pull point
            if (node.subscription && node.subscription.unsubscribe) {
                node.subscription.unsubscribe(function(err) {
                    if (err) {
                        console.log("Error unsubscribing on close: " + err);
                    }
                });
            }
            
            node.subscription = null;
            node.processEventMessage = null;
        });
    }
    RED.nodes.registerType("onvif-events",OnVifEventsNode);
}
