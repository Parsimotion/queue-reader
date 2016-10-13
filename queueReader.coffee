Promise = require("bluebird")
async = require("async")
_ = require("lodash")
module.exports =

class QueueReader

  constructor: (@service) ->

  _buildQueue: (process) =>
    async.queue (message, callback) =>
      body = message.body
      deleteMessage = =>
        console.log "Message processed OK"
        @service.deleteMessage(message)

      promise = if body?
        process(body).then deleteMessage
      else
        deleteMessage()

      promise
      .catch -> console.log "Message #{message.messageId} failed"
      .finally () => callback()
    , 5

  run: (receiver) =>
    @queue = @_buildQueue receiver
    @service.createQueueIfNotExists().then =>
      console.log  "Listening for messages..."
      @_receive()

  _receive: =>
    @service.getMessages()
    .then (messages) =>
      postpone = -> new Promise (resolve) -> setTimeout(resolve, 5000)
      return postpone() if _.isEmpty messages
      console.log  "Receiving messages..."
      @queue.push messages, () ->
      return postpone() if @queue.length() > 25
      Promise.resolve()
    .finally => @_receive()

