/* jshint node:true */
'use strict';

var Engine = function(server, options) {
  this._server  = server;
  this._options = options || {};
  this._disconnected = false;
  var gc               = this._options.gc       || this.DEFAULT_GC,
      client           = this._options.client,
      subscriberClient = this._options.subscriberClient,
      writeOnly        = this._options.writeOnly;

  this._ns  = this._options.namespace || '';

  var RedisClass;
  if (!client || !subscriberClient) {
    RedisClass = this._options.redisClass || require('ioredis');
  }

  if (client) {
    this._redis = client;
  } else {
    this._redis = new RedisClass(this._options.redisConnectionOptions);
  }

  if (!writeOnly) {
    if (subscriberClient) {
      this._subscriber = subscriberClient;
    } else {
      this._subscriber = new RedisClass(this._options.redisConnectionOptions);
    }
  }


  this._messageChannel = this._ns + '{shared}/notifications/messages';
  this._closeChannel   = this._ns + '{shared}/notifications/close';

  this._clientsKey = this._ns + '{shared}/clients';

  var self = this;

  if (this._subscriber) {
    this._subscriber.subscribe(this._messageChannel);
    this._subscriber.subscribe(this._closeChannel);
    this._subscriber.on('message', function(topic, message) {
      if (topic === self._messageChannel) self.emptyQueue(message);
      if (topic === self._closeChannel)   self._server.trigger('close', message);
    });
  }

  this._gc = setInterval(function() { self.gc(); }, gc * 1000);
};

Engine.create = function(server, options) {
  return new this(server, options);
};

Engine.prototype = {
  DEFAULT_GC:       60,
  LOCK_TIMEOUT:     120,

  disconnect: function() {
    this._disconnected = true;
    clearInterval(this._gc);

    const actions = [
      this._redis.disconnect(),
    ];

    if (this._subscriber) {
      actions.push(
        this._subscriber.disconnect()
      );
    }

    return Promise.all(actions).catch(function(err) {
      if (err) console.error(err.stack);
      throw err;
    });

  },

  _newClientId: async function() {
    var clientId = this._server.generateId();
    var timeout = await this._redis.zscore(this._clientsKey, clientId);
    if (timeout !== null) return this._newClientId();
    return clientId;
  },

  createClient: function(callback, context) {
    return this._newClientId()
      .then((clientId) => {
        return Promise.resolve(this._updateClientTTL(clientId)).then(() => {
          this._server.debug('Created new client ?', clientId);
          this._server.trigger('handshake', clientId);
          return clientId;
        });
      })
      .then((clientId) => {
        if (callback) callback.call(context, clientId);
        return clientId;
      })
      .catch((err) => {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
        throw err;
      });
  },

  clientExists: function(clientId, callback, context) {
    this._clientExists(clientId).then((exists) => {
      callback.call(context, exists);
    });
  },

  _clientExists: async function(clientId) {
    const score = await this._redis.zscore(this._clientsKey, clientId);
    return !this._clientExpired(score);
  },

  _clientExpired: function(score) {
    if (!score)
      return true;

    const cutoff = new Date().getTime() - (1000 * 1.6 * this._server.timeout);
    return parseInt(score, 10) <= cutoff;
  },

  destroyClient: async function(clientId, callback, context) {
    try {
      var channels = await this._redis.smembers(this._clientChannelsKey(clientId));
      var multiClient = this._redis.multi();

      channels.forEach(function(channel) {
        multiClient.srem(this._clientChannelsKey(clientId), channel);
      }, this);

      multiClient.del(this._clientMessagesKey(clientId));

      var results = await multiClient.exec();

      channels.forEach(function(channel, i) {
        var result = results[i][1];
        if (result !== 1) return;
        this._server.trigger('unsubscribe', clientId, channel);
        this._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
      }, this);

      var multi = this._redis.multi();

      channels.forEach(function(channel) {
        multi.srem(this._channelKey(channel), clientId);
      }, this);

      multi.zrem(this._clientsKey, clientId);
      multi.publish(this._closeChannel, clientId);

      await multi.exec();

      this._server.debug('Destroyed client ?', clientId);
      this._server.trigger('disconnect', clientId);
    } catch (err) {
      if (err) console.error(err.stack);
      throw err;
    } finally {
      if (callback) callback.call(context);
    }
  },

  _updateClientTTL: function(clientId) {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    var time = new Date().getTime();

    this._server.debug('Ping ?, ?', clientId, time);
    return this._redis.zadd(this._clientsKey, time, clientId);
  },

  ping: function(clientId) {
    this._updateClientTTL(clientId);
  },

  subscribe: function(clientId, channel, callback, context) {
    return Promise.all([
        this._redis.sadd(this._clientChannelsKey(clientId), channel),
        this._redis.sadd(this._channelKey(channel), clientId)
      ])
      .then(([added]) => {
        if (added === 1) this._server.trigger('subscribe', clientId, channel);
        this._server.debug('Subscribed client ? to channel ?', clientId, channel);
      })
      .then(() => {
        if (callback) callback.call(context);
      })
      .catch((err) => {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
        throw err;
      });
  },

  unsubscribe: function(clientId, channel, callback, context) {
    return Promise.all([
        this._redis.srem(this._clientChannelsKey(clientId), channel),
        this._redis.srem(this._channelKey(channel), clientId)
      ])
      .then(([removed]) => {
        if (removed === 1) this._server.trigger('unsubscribe', clientId, channel);
        this._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
      })
      .then(() => {
        if (callback) callback.call(context);
      })
      .catch((err) => {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
        throw err;
      });
  },

  publish: function(message, channels) {
    this._server.debug('Publishing message ?', message);

    const jsonMessage = JSON.stringify(message),
          keys        = channels.map(this._channelKey, this);

    this._server.trigger('publish', message.clientId, message.channel, message.data);

    return this._redis.sunion(keys).then(async (clients) => {
      if (!clients || !clients.length) return;

      for (let clientId of clients) {
        const queue = this._clientMessagesKey(clientId);
        this._server.debug('Queueing for client ?: ?', clientId, message);

        const pipeline = this._redis.pipeline();

        pipeline.rpush(queue, jsonMessage);
        pipeline.publish(this._messageChannel, clientId);
        pipeline.zscore(this._clientsKey, clientId);

        const res = await pipeline.exec();
        const clientScore = res[2] && res[2][1];

        if (typeof clientScore !== 'undefined' && (clientScore === null || this._clientExpired(clientScore)))
            await this._redis.del(queue);
      }
    });
  },

  emptyQueue: function(clientId) {
    if (!this._server.hasConnection(clientId)) return;

    var key   = this._clientMessagesKey(clientId),
        multi = this._redis.multi();

    multi.lrange(key, 0, -1);
    multi.del(key);

    return multi.exec()
      .then((results) => {
        var jsonMessages = results[0][1];
        if (!jsonMessages) return;
        var messages = jsonMessages.map(function(json) { return JSON.parse(json); });
        this._server.deliver(clientId, messages);
      });
  },

  gc: async function() {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;
    if (this._disconnected) return;

    return this._withLock('gc', async function() {
      var cutoff = new Date().getTime() - 1000 * 2 * timeout;
      var clients = await this._redis.zrangebyscore(this._clientsKey, 0, cutoff);
      await Promise.all(clients.map((clientId) => this.destroyClient(clientId)));
    }, this);
  },

  _withLock: function(lockName, callback, context) {
    var lockKey     = this._lockKey(lockName),
        currentTime = new Date().getTime(),
        expiry      = currentTime + this.LOCK_TIMEOUT * 1000 + 1;

    return this._redis.setnx(lockKey, expiry)
      .then((set) => {
        if (this._disconnected) return false;

        if (set === 1) {
          return true;
        }

        return this._redis.get(lockKey)
          .then((timeout) => {
            if (this._disconnected) return false;
            if (!timeout) return false;

            var lockTimeout = parseInt(timeout, 10);
            if (currentTime < lockTimeout) return false;

            return this._redis.getset(lockKey, expiry)
              .then((oldValue) => {
                if (this._disconnected) return false;

                if (oldValue !== timeout) return false;
                return true;
              });
          });
      })
      .then((lockObtained) => {
        if (!lockObtained) return;

        var callbackContext = context || this;

        return Promise.resolve()
          .then(function() {
            return callback.call(callbackContext);
          })
          .finally(() => {
            if (this._disconnected) return;
            if (new Date().getTime() < expiry) this._redis.del(lockKey);
          });
      });
  },

  _lockKey: function(lockName) {
    return this._ns + '{shared}/locks/' + lockName;
  },

  _channelKey: function(channel) {
    return this._ns + '{shared}/channels' + channel;
  },

  _clientChannelsKey: function(clientId) {
    return this._ns + '/clients/{' + clientId + '}/channels';
  },

  _clientMessagesKey: function(clientId) {
    return this._ns + '/clients/{' + clientId + '}/messages';
  }
};

module.exports = Engine;
