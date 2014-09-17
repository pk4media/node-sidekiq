var Promise, Sidekiq, crypto, _;

Promise = require('bluebird');
crypto = Promise.promisifyAll(require("crypto"));
_ = require("lodash");

Sidekiq = (function() {
  var generateJobId, getQueueName, jobIndex;

  function Sidekiq(redisConnection, namespace) {
    this.redisConnection = redisConnection;
    this.namespace = namespace;
    this.redisConnection = Promise.promisifyAll(this.redisConnection);
  }

  Sidekiq.prototype.enqueue = Promise.coroutine(function*(workerClass, args, payload, restrictDuplicateScheduling) {
    payload["class"] = workerClass;
    payload.args = args;
    payload.jid = yield generateJobId();

    if (payload.at instanceof Date) {
      payload.at = payload.at.getTime() / 1000;
      return this.redisConnection.zaddAsync(this.namespaceKey('schedule'), payload.at, JSON.stringify(payload));
    
    } else if (!restrictDuplicateScheduling || !(yield this.checkJobScheduling(payload))) {   
      
      this.redisConnection.lpush(this.getQueueKey(payload.queue), JSON.stringify(payload));
      return this.redisConnection.saddAsync(this.namespaceKey("queues"), getQueueName(payload.queue));      
    };
  });

  Sidekiq.prototype.checkJobScheduling = function(payload) {
    return this.redisConnection.zrangeAsync('schedule', 0, -1)
    .then(function(jobs) {
      var jobLocation = jobIndex(jobs, payload);

      return jobLocation > -1;
    });
  };

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

  generateJobId = function() {
    return crypto.randomBytesAsync(12);
  };

  getQueueName = function(queueName) {
    return queueName || "default";
  };

  jobIndex = function(jobs, payload) {
    return jobs.findIndex(function(job) {
      var jobInfo = JSON.parse(job);
      return jobInfo['class'] === payload['class'] && _.difference(jobInfo['args'], payload['args']).length === 0;
    });
  };

  module.exports = Sidekiq;

  return Sidekiq;

})();
