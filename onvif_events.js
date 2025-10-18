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
                        
                        // Create pull point subscription for Tapo camera compatibility
                        node.deviceConfig.cam.createPullPointSubscription(function(err, subscription) {
                            if (err) {
                                node.error("Failed to create pull point subscription: " + err);
                                return;
                            }
                            
                            node.subscription = subscription;
                            node.stopPulling = false;
                            node.errorCount = 0; // For exponential backoff
                            
                            // Overwrite the device status text
                            node.status({fill:"green",shape:"dot",text:"listening"}); 
                            
                            // DEFINE processEventMessage BEFORE starting polling loop to avoid race condition
                            node.processEventMessage = function(camMessage) {
                                try {
                                    // Defensive null-checks for message shapes
                                    if (!camMessage || !camMessage.topic || !camMessage.message || 
                                        !camMessage.message.message || !camMessage.message.message.$) {
                                        console.log("Received malformed event message, skipping");
                                        return;
                                    }
                                    
                                    var eventTopic = camMessage.topic._ || camMessage.topic;
                                    
                                    // Handle topic as string directly if it's not an object
                                    if (typeof eventTopic === 'string') {
                                        // Strip the namespaces from the topic (e.g. tns1:MediaControl/tnsavg:ConfigurationUpdateAudioEncCfg)
                                        // Split on '/', then remove any namespace for each part, and at the end recombine parts that were split with '/'
                                        let parts = eventTopic.split('/');
                                        eventTopic = "";
                                        for (var index = 0; index < parts.length; index++) {
                                            var stringNoNamespace = parts[index].split(':').pop();
                                            if (eventTopic.length == 0) {
                                                eventTopic += stringNoNamespace;
                                            } else {
                                                eventTopic += '/' + stringNoNamespace;
                                            }
                                        }
                                    }

                                    var outputMsg = {
                                        topic: eventTopic,
                                        time: camMessage.message.message.$.UtcTime,
                                        property: camMessage.message.message.$.PropertyOperation // Initialized, Deleted or Changed but missing/undefined on the Avigilon 4 channel encoder
                                    };

                                    // Only handle simpleItem
                                    // Only handle one 'source' item
                                    // Ignore the 'key' item  (nothing I own produces it)
                                    // Handle all the 'Data' items

                                    // SOURCE (Name:Value)
                                    if (camMessage.message.message.source && camMessage.message.message.source.simpleItem) {
                                        if (Array.isArray(camMessage.message.message.source.simpleItem)) {
                                            // TODO : currently we only process the first event source item ...
                                            outputMsg.source = {
                                                name:  camMessage.message.message.source.simpleItem[0].$.Name,
                                                value: camMessage.message.message.source.simpleItem[0].$.Value
                                            }
                                        }
                                        else {
                                            outputMsg.source = {
                                                name: camMessage.message.message.source.simpleItem.$.Name,
                                                value: camMessage.message.message.source.simpleItem.$.Value
                                            }
                                        }
                                    }
                                    
                                    //KEY
                                    if (camMessage.message.message.key) {
                                        outputMsg.key = camMessage.message.message.key;
                                    }

                                    // DATA (Name:Value)
                                    if (camMessage.message.message.data && camMessage.message.message.data.simpleItem) {
                                        if (Array.isArray(camMessage.message.message.data.simpleItem)) {
                                            outputMsg.data = [];
                                            for (var x  = 0; x < camMessage.message.message.data.simpleItem.length; x++) {
                                                outputMsg.data.push({
                                                    name: camMessage.message.message.data.simpleItem[x].$.Name,
                                                    value: camMessage.message.message.data.simpleItem[x].$.Value
                                                })
                                            }
                                        }
                                        else {
                                            outputMsg.data = {
                                                name: camMessage.message.message.data.simpleItem.$.Name,
                                                value: camMessage.message.message.data.simpleItem.$.Value
                                            }
                                        }
                                    }
                                    else if (camMessage.message.message.data && camMessage.message.message.data.elementItem) {
                                        outputMsg.data = {
                                            dataName: 'elementItem',
                                            dataValue: JSON.stringify(camMessage.message.message.data.elementItem)
                                        }
                                    }

                                    // As soon as we get an event from the camera, we will send it to the output of this node
                                    node.send(outputMsg);
                                } catch (err) {
                                    console.log("Error processing event message: " + err);
                                }
                            };
                            
                            // Function to actively pull messages from the camera
                            var pullMessages = function() {
                                if (!node.subscription || node.stopPulling) {
                                    return;
                                }
                                
                                // Pull messages with 1 second timeout and max 100 messages
                                node.subscription.pullMessages({
                                    timeout: 'PT1S',
                                    messageLimit: 100
                                }, function(err, result) {
                                    if (err) {
                                        // Only log error if not stopped intentionally
                                        if (!node.stopPulling) {
                                            node.errorCount = (node.errorCount || 0) + 1;
                                            console.log("Error pulling messages (attempt " + node.errorCount + "): " + err);
                                            
                                            // Exponential backoff: 1s, 2s, 4s, 8s, max 30s
                                            var backoffDelay = Math.min(1000 * Math.pow(2, node.errorCount - 1), 30000);
                                            setTimeout(pullMessages, backoffDelay);
                                        }
                                        return;
                                    }
                                    
                                    // Reset error count on success
                                    node.errorCount = 0;
                                    
                                    // Process notification messages
                                    if (result && result.notificationMessage) {
                                        var messages = Array.isArray(result.notificationMessage) 
                                            ? result.notificationMessage 
                                            : [result.notificationMessage];
                                        
                                        messages.forEach(function(notifMsg) {
                                            // Convert notification message to the expected format
                                            var camMessage = {
                                                topic: notifMsg.topic,
                                                message: notifMsg.message
                                            };
                                            
                                            // Process using the event handler logic
                                            if (node.processEventMessage) {
                                                node.processEventMessage(camMessage);
                                            }
                                        });
                                    }
                                    
                                    // Continue polling
                                    if (!node.stopPulling) {
                                        setImmediate(pullMessages);
                                    }
                                });
                            };
                            
                            // Call SetSynchronizationPoint to get current property states (critical for Tapo cameras)
                            if (node.subscription.setSynchronizationPoint) {
                                node.subscription.setSynchronizationPoint(function(err) {
                                    if (err) {
                                        console.log("Note: setSynchronizationPoint returned: " + err);
                                    }
                                    // Start polling regardless of sync result
                                    pullMessages();
                                });
                            } else {
                                // Start polling immediately if sync not supported
                                pullMessages();
                            }
                            
                            // Set up subscription renewal to prevent expiry (every 60 seconds)
                            const RENEW_INTERVAL_MS = 60 * 1000;
                            node.renewalTimer = setInterval(function() {
                                if (node.subscription && node.subscription.renew) {
                                    node.subscription.renew(function(err) {
                                        if (err) {
                                            console.log("Error renewing subscription: " + err);
                                        }
                                    });
                                }
                            }, RENEW_INTERVAL_MS);
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
