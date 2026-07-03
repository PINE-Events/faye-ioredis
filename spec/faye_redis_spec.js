var RedisEngine = require('../faye-redis')
var Redis = require('ioredis');

var REDIS_CLUSTER_HOST = process.env.REDIS_CLUSTER_HOST || '127.0.0.1';
var REDIS_CLUSTER_PORTS = (process.env.REDIS_CLUSTER_PORTS || '7000,7001')
  .split(',')
  .map(function(port) { return parseInt(port.trim(), 10); })
  .filter(function(port) { return !isNaN(port); });

if (REDIS_CLUSTER_PORTS.length < 2) {
  throw new Error('REDIS_CLUSTER_PORTS must contain at least two ports, e.g. 7100,7101');
}

JS.Test.describe("Redis engine", function() { with(this) {
  before(function() {
    this.engineOpts = {
      type: RedisEngine,
      redisClass: Redis.Cluster,
      redisConnectionOptions: [{
        port: REDIS_CLUSTER_PORTS[0],
        host: REDIS_CLUSTER_HOST
      }, {
        port: REDIS_CLUSTER_PORTS[1],
        host: REDIS_CLUSTER_HOST
      }],
      namespace: new Date().getTime().toString() }

  })

  after(function(resume) { with(this) {
    disconnect_engine()
    resume();
  }})

  itShouldBehaveLike("faye engine")

  describe("distribution", function() { with(this) {
    itShouldBehaveLike("distributed engine")
  }})

  if (process.env.TRAVIS) return

  describe("using a Unix socket", function() { with(this) {
    before(function() { with(this) {
      this.engineOpts.socket = "/tmp/redis.sock"
    }})

    itShouldBehaveLike("faye engine")
  }})
}})
