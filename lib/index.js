var Sidekiq, crypto;

crypto = require("crypto");

Sidekiq = (function() {
  var generateJobId, getQueueName;

  generateJobId = function(cb) {
    return crypto.randomBytes(12, function(err, buf) {
      if (err != null) {
        return cb(err);
      }
      return cb(null, buf.toString("hex"));
    });
  };

  getQueueName = function(queueName) {
    return queueName || "default";
  };

  function Sidekiq(redisConnection, namespace) {
    this.redisConnection = redisConnection;
    this.namespace = namespace;
  }

  Sidekiq.prototype.namespaceKey = function(key) {
    if (this.namespace != null) {
      return "" + this.namespace + ":" + key;
    } else {
      return key;
    }
  };

  Sidekiq.prototype.getQueueKey = function(queueName) {
    return this.namespaceKey("queue:" + (getQueueName(queueName)));
  };

  Sidekiq.prototype.jobIsUnique = function(payload) {
    return this.redisConnection.lindex(this.getQueueKey(payload.queue), JSON.stringify(payload)) === -1;
  };

  Sidekiq.prototype.enqueue = function(workerClass, args, payload, enforce_uniqueness) {
    var _this = this;
    return generateJobId(function(err, jid) {
      payload["class"] = workerClass;
      payload.args = args;
      payload.jid = jid;
      if (payload.at instanceof Date) {
        payload.at = payload.at.getTime() / 1000;
        return _this.redisConnection.zadd(_this.namespaceKey("schedule"), payload.at, JSON.stringify(payload));
      } else {
        if (!enforce_uniqueness || _this.jobIsUnique(payload)) {
          _this.redisConnection.lpush(_this.getQueueKey(payload.queue), JSON.stringify(payload));
          return _this.redisConnection.sadd(_this.namespaceKey("queues"), getQueueName(payload.queue));
        }
      }
    });
  };

  module.exports = Sidekiq;

  return Sidekiq;

})();
