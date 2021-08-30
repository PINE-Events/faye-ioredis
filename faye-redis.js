/* jshint node:true */
'use strict';

var Promise = require('bluebird');

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

    return Promise.all(actions)
    .nodeify(function(err) {
      if (err) console.error(err.stack);
    });

  },

  _newClientId: function() {
    var clientId = this._server.generateId();
    return this._redis.zscore(this._clientsKey, clientId)
      .bind(this)
      .then(function(timeout) {
        if (timeout !== null) return this._newClientId();
        return clientId;
      });
  },

  createClient: function(callback, context) {
    return this._newClientId()
      .bind(this)
      .then(function(clientId) {
        return this._updateClientTTL(clientId)
          .bind(this)
          .then(function() {
            this._server.debug('Created new client ?', clientId);
            this._server.trigger('handshake', clientId);
            return clientId;
          });
      })
      .nodeify(function(err, clientId) {
        if (err) console.error(err.stack);
        callback.call(context, clientId);
      });
  },

  clientExists: function(clientId, callback, context) {
    var cutoff = new Date().getTime() - (1000 * 1.6 * this._server.timeout);

    this._redis.zscore(this._clientsKey, clientId, function(error, score) {
      callback.call(context, parseInt(score, 10) > cutoff);
    });
  },

  destroyClient: function(clientId, callback, context) {
    return this._redis.smembers(this._clientChannelsKey(clientId))
      .bind(this)
      .then(function(channels) {
        var multiClient = this._redis.multi();

        channels.forEach(function(channel) {
          multiClient.srem(this._clientChannelsKey(clientId), channel);
        }, this);

        multiClient.del(this._clientMessagesKey(clientId));

        return [channels, multiClient.exec()];
      })
      .spread(function(channels, results) {
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

        return multi.exec();
      })
      .then(function() {
        this._server.debug('Destroyed client ?', clientId);
        this._server.trigger('disconnect', clientId);
      })
      .nodeify(function(err) {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
      });
  },

  _updateClientTTL: Promise.method(function(clientId) {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    var time = new Date().getTime();

    this._server.debug('Ping ?, ?', clientId, time);
    return this._redis.zadd(this._clientsKey, time, clientId);
  }),

  ping: function(clientId) {
    this._updateClientTTL(clientId);
  },

  subscribe: function(clientId, channel, callback, context) {
    return Promise.all([
        this._redis.sadd(this._clientChannelsKey(clientId), channel),
        this._redis.sadd(this._channelKey(channel), clientId)
      ])
      .bind(this)
      .spread(function(added) {
        if (added === 1) this._server.trigger('subscribe', clientId, channel);
        this._server.debug('Subscribed client ? to channel ?', clientId, channel);
      })
      .nodeify(function(err) {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
      });
  },

  unsubscribe: function(clientId, channel, callback, context) {
    return Promise.all([
        this._redis.srem(this._clientChannelsKey(clientId), channel),
        this._redis.srem(this._channelKey(channel), clientId)
      ])
      .bind(this)
      .spread(function(removed) {
        if (removed === 1) this._server.trigger('unsubscribe', clientId, channel);
        this._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
      })
      .nodeify(function(err) {
        if (err) console.error(err.stack);
        if (callback) callback.call(context);
      });
  },

  publish: function(message, channels) {
    this._server.debug('Publishing message ?', message);

    var jsonMessage = JSON.stringify(message),
        keys        = channels.map(this._channelKey, this);

    this._server.trigger('publish', message.clientId, message.channel, message.data);

    return this._redis.sunion(keys)
      .bind(this)
      .then(function(clients) {
        if (!clients || !clients.length) return;

        return Promise.map(clients, function(clientId) {
            var queue = this._clientMessagesKey(clientId);
            this._server.debug('Queueing for client ?: ?', clientId, message);

            return this._redis.rpush(queue, jsonMessage);
            // TODO: figure out what to do with this
            // this.clientExists(clientId, function(exists) {
            //   if (!exists) this._redis.del(queue);
            // });
          }.bind(this))
          .bind(this)
          .then(function() {
            var pipeline = this._redis.pipeline();
            var messageChannel = this._messageChannel;
            clients.forEach(function(clientId) {
              pipeline.publish(messageChannel, clientId);
            });

            return pipeline.exec();
          });
      });
  },

  emptyQueue: function(clientId) {
    if (!this._server.hasConnection(clientId)) return;

    var key   = this._clientMessagesKey(clientId),
        multi = this._redis.multi();

    multi.lrange(key, 0, -1);
    multi.del(key);

    return multi.exec()
      .bind(this)
      .then(function(results) {
        var jsonMessages = results[0][1];
        if (!jsonMessages) return;
        var messages = jsonMessages.map(function(json) { return JSON.parse(json); });
        this._server.deliver(clientId, messages);
      });
  },

  gc: function() {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;
    if (this._disconnected) return;

    return this._withLock('gc', function() {
      var cutoff = new Date().getTime() - 1000 * 2 * timeout;

      return this._redis.zrangebyscore(this._clientsKey, 0, cutoff)
        .bind(this)
        .then(function(clients) {
          return Promise.map(clients, function(clientId) {
            return this.destroyClient(clientId);
          }.bind(this));
        });
    }.bind(this));
  },

  _withLock: function(lockName, callback, context) {
    var lockKey     = this._lockKey(lockName),
        currentTime = new Date().getTime(),
        expiry      = currentTime + this.LOCK_TIMEOUT * 1000 + 1;

    return this._redis.setnx(lockKey, expiry)
      .bind(this)
      .then(function(set) {
        if (this._disconnected) return false;

        if (set === 1) {
          return true;
        }

        return this._redis.get(lockKey)
          .bind(this)
          .then(function(timeout) {
            if (this._disconnected) return false;
            if (!timeout) return false;

            var lockTimeout = parseInt(timeout, 10);
            if (currentTime < lockTimeout) return false;

            return this._redis.getset(lockKey, expiry)
              .bind(this)
              .then(function(oldValue) {
                if (this._disconnected) return false;

                if (oldValue !== timeout) return false;
                return true;
              });
          });
      })
      .then(function(lockObtained) {
        if (!lockObtained) return;

        return Promise.try(callback)
          .bind(this)
          .finally(function() {
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
