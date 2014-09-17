crypto = require "crypto"

class Sidekiq
  generateJobId = (cb) ->
    crypto.randomBytes 12, (err, buf) ->
      return cb(err) if err?
      cb null, buf.toString("hex")

  getQueueName = (queueName) ->
    queueName or "default"

  constructor: (@redisConnection, @namespace) ->

  namespaceKey: (key) ->
    if @namespace? then "#{@namespace}:#{key}" else key

  getQueueKey: (queueName) ->
    @namespaceKey "queue:#{getQueueName(queueName)}"

  jobIsUnique: (payload) ->
    @redisConnection.lindex(@getQueueKey(payload.queue), JSON.stringify(payload)) == -1

  enqueue: (workerClass, args, payload, enforce_uniqueness) ->
    generateJobId (err, jid) =>
      # Build job payload
      payload.class = workerClass
      payload.args = args
      payload.jid = jid

      if payload.at instanceof Date
        payload.at = payload.at.getTime() / 1000
        # Push job payload to schedule
        @redisConnection.zadd @namespaceKey("schedule"), payload.at, JSON.stringify(payload)
      else
        if !enforce_uniqueness or @jobIsUnique(payload)
          # Push job payload to redis
          @redisConnection.lpush @getQueueKey(payload.queue), JSON.stringify(payload)

          # Create the queue if it doesn't already exist
          @redisConnection.sadd @namespaceKey("queues"), getQueueName(payload.queue)

  module.exports = Sidekiq
