// cns-padi.js - CNS Padi Transport
// Copyright 2023 Padi, Inc. All Rights Reserved.

'use strict';

// Imports

const cns = require('cns-sdk');
const mqtt = require('mqtt');

// Classes

const Application = cns.Application;
const Connection = cns.Connection;
const Transport = cns.Transport;

// Padi transport

class Padi extends Transport {

  // Properties

  api = 'https://api.padi.io';
  broker = 'wss://cns.padi.io:1881';
  sub = {qos: 0, rap: true};
  pub = {retain: true};
  token;
  client;

  // Constructor

  // Create new transport
  constructor(options) {
    super(options);
  }

  // Methods

  // Open transport
  open(options) {
    // Close previous
    this.close();

    // Copy options
    options = options || {};

    if (options.api) this.api = options.api;
    if (options.broker) this.broker = options.broker;
    if (options.token) this.token = options.token;

    // Create mqtt client
    this.client = mqtt.connect(this.broker, {
      username: this.token
    })
    // Connection established
    .on('connect', () => {
      super.open();
    })
    // Topic changed
    .on('message', (topic, message, packet) => {
      this.receive(topic, message);
    })
    // Connection closed
    .on('close', () => {
      super.close();
    })
    // Client error
    .on('error', (error) => {
      super.error(error);
    });

    return this;
  }

  // Send packet
  send(packet) {
    //
    const topic = 'thing/' + packet.identifier.id;

    switch (packet.action) {
      case Transport.SUBSCRIBE:
        // Subscribe
        this.client.subscribe(topic, this.sub, (error) => {
          if (error) this.application.error(Application.E_SUBSCRIBE, error);
        });
        break;
      case Transport.PUBLISH:
        // Publish
        this.client.publish(topic, packet.data, this.pub, (error) => {
          if (error) this.application.error(Application.E_PUBLISH, error);
        });
        break;
      case Transport.UNSUBSCRIBE:
        // Unsubscribe
        this.client.unsubscribe(topic, (error) => {
          if (error) this.application.error(Application.E_UNSUBSCRIBE, error);
        });
        break;
    }
    return this;
  }

  // Receive packet
  receive(topic, message) {
    const id = topic.replace('thing/', '');
    const packet = this.parse(message);

    const properties = packet.padiThings[id];
    const connections = {};

    for (var n in packet.padiConnections) {
      const connection = packet.padiConnections[n];

      const client = connection.padiClient;
      const server = connection.padiServer;

      const role = (id === client)?
        Connection.CLIENT:
        Connection.SERVER;

      connections[n] = {
        identifier: {
          id: n
        },
        profile: connection.padiProfile,
        version: connection.padiVersion,
        role: role,
        status: connection.padiStatus,
        client: connection.padiClientAlias,
        server: connection.padiServerAlias,
        properties: connection.padiProperties
      };
    }

    super.message({
      action: Transport.MESSAGE,
      identifier: {
        id: id
      },
      properties: properties,
      connections: connections
    });
  }

  // Close transport
  close() {
    if (this.client) {
      this.client.end();
      this.client = undefined;
    }
    return this;
  }
}

// Exports

exports.Transport = Padi;
