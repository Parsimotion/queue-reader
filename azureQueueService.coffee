azure = require("azure-storage")
Promise = require("bluebird")
_ = require("lodash")

getQueueSvc = _.once ->
  Promise.promisifyAll azure.createQueueService()

module.exports =

class AzureQueueService
  constructor: (@queueName) ->

  createQueueIfNotExists: =>
    getQueueSvc().createQueueIfNotExistsAsync @queueName

  send: (message) =>
    getQueueSvc().createMessageAsync @queueName, JSON.stringify message

  getMessages: =>
    getQueueSvc().getMessagesAsync(@queueName, {numOfMessages: 5, visibilityTimeout: 5 * 60}).then (messages) ->
      _.compact messages.map (it) ->
        it.body = try JSON.parse it.messageText
        it

  deleteMessage: (message) =>
    getQueueSvc().deleteMessageAsync(@queueName, message.messageId, message.popReceipt)

