/****************************************************************************
**
** Copyright (C) 2017 The Qt Company Ltd.
** Contact: https://www.qt.io/licensing/
**
** This file is part of the QtWebChannel module of the Qt Toolkit.
**
** $QT_BEGIN_LICENSE:BSD$
** Commercial License Usage
** Licensees holding valid commercial Qt licenses may use this file in
** accordance with the commercial license agreement provided with the
** Software or, alternatively, in accordance with the terms contained in
** a written agreement between you and The Qt Company. For licensing terms
** and conditions see https://www.qt.io/terms-conditions. For further
** information use the contact form at https://www.qt.io/contact-us.
**
** BSD License Usage
** Alternatively, you may use this file under the terms of the BSD license
** as follows:
**
** "Redistribution and use in source and binary forms, with or without
** modification, are permitted provided that the following conditions are
** met:
**   * Redistributions of source code must retain the above copyright
**     notice, this list of conditions and the following disclaimer.
**   * Redistributions in binary form must reproduce the above copyright
**     notice, this list of conditions and the following disclaimer in
**     the documentation and/or other materials provided with the
**     distribution.
**   * Neither the name of The Qt Company Ltd nor the names of its
**     contributors may be used to endorse or promote products derived
**     from this software without specific prior written permission.
**
**
** THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
** "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
** LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
** A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
** OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
** SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
** LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
** DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
** THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
** (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
** OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."
**
** $QT_END_LICENSE$
**
****************************************************************************/

"use strict";

class QWebChannel {
    constructor(transport, initCallback) {
        if (typeof transport !== "object" || typeof transport.send !== "function") {
            throw new Error("The QWebChannel transport object is missing or invalid.");
        }
        this.transport = transport;
        this.send = this.send.bind(this);
        this.transport.onmessage = this.receive.bind(this);
        this.execCallbacks = {};
        this.execId = 0;
        this.objects = {};
        this.send("initialize");
        if (initCallback) {
            initCallback(this);
        }
    }

    send(data) {
        if (typeof data !== "string") {
            data = JSON.stringify(data);
        }
        this.transport.send(data);
    }

    receive(data) {
        let message = JSON.parse(data);
        if (typeof message !== "object") {
            console.error("Invalid message received: " + data);
            return;
        }

        switch (message.type) {
            case "response":
                this.handleResponse(message);
                break;
            case "signal":
                this.handleSignal(message);
                break;
            case "propertyUpdate":
                this.handlePropertyUpdate(message);
                break;
            default:
                if (message.object) {
                    this.objects[message.object] = new QObject(message.object, message.data, this);
                    if (this.objects.self) {
                        this.objects.self.init();
                    }
                } else {
                    console.error("Invalid message received: ", message);
                }
                break;
        }
    }

    exec(data, callback) {
        if (!callback) {
            this.send(data);
            return;
        }
        let execId = ++this.execId;
        this.execCallbacks[execId] = callback;
        data.id = execId;
        this.send(data);
    }

    handleResponse(message) {
        if (!message.id || !this.execCallbacks[message.id]) {
            console.error("Received response for unknown exec id: " + message.id);
            return;
        }
        this.execCallbacks[message.id](message.data);
        delete this.execCallbacks[message.id];
    }

    handleSignal(message) {
        let object = this.objects[message.object];
        if (object) {
            object.signalEmitted(message.signal, message.args);
        }
    }

    handlePropertyUpdate(message) {
        for (let i in message.data) {
            let object = this.objects[i];
            if (object) {
                object.propertyUpdate(message.data[i]);
            }
        }
    }
}

class QObject {
    constructor(name, data, webChannel) {
        this.id = name;
        this.webChannel = webChannel;
        this.methods = {};
        this.properties = {};
        this.signals = {};
        for (let i in data.methods) {
            this.methods[i] = this.createMethod(i, data.methods[i]);
        }
        for (let i in data.properties) {
            this.properties[i] = data.properties[i];
        }
        for (let i in data.signals) {
            this.signals[i] = new QSignal(i, data.signals[i], this);
        }
    }

    createMethod(name, argCount) {
        let object = this;
        return function () {
            let args = [];
            for (let i = 0; i < argCount; ++i) {
                if (i < arguments.length) {
                    args.push(arguments[i]);
                } else {
                    args.push(undefined);
                }
            }
            let data = {
                type: "invokeMethod",
                object: object.id,
                method: name,
                args: args
            };
            if (arguments.length > argCount) {
                object.webChannel.exec(data, arguments[argCount]);
            } else {
                object.webChannel.exec(data);
            }
        };
    }

    propertyUpdate(data) {
        for (let i in data) {
            let property = this.properties[i];
            if (property && property.notify) {
                property.notify.apply(property, data[i]);
            }
        }
    }

    signalEmitted(signalName, args) {
        if (this.signals[signalName]) {
            this.signals[signalName].emitted.apply(this.signals[signalName], args);
        }
    }
}

class QSignal {
    constructor(name, argTypes, object) {
        this.name = name;
        this.argTypes = argTypes;
        this.object = object;
        this.callbacks = [];
    }

    connect(callback) {
        if (typeof callback !== "function") {
            throw new Error("Bad callback given to connect to signal " + this.name);
        }
        this.callbacks.push(callback);
    }

    disconnect(callback) {
        for (let i = 0; i < this.callbacks.length; ++i) {
            if (this.callbacks[i] === callback) {
                this.callbacks.splice(i, 1);
                return;
            }
        }
    }

    emitted() {
        for (let i = 0; i < this.callbacks.length; ++i) {
            this.callbacks[i].apply(null, arguments);
        }
    }
}