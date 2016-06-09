_                   = require 'lodash'
async               = require 'async'
uuid                = require 'uuid'
http                = require 'http'
SubscriptionManager = require 'meshblu-core-manager-subscription'

class DeliverSubscriptions
  constructor: (options={}) ->
    {
      @firehoseClient
      datastore
      @uuidAliasResolver
      @jobManager
    } = options
    @subscriptionManager ?= new SubscriptionManager {datastore, @uuidAliasResolver}

  _createJob: ({messageType, jobType, toUuid, message, fromUuid, auth}, callback) =>
    request =
      data: message
      metadata:
        auth: auth
        toUuid: toUuid
        fromUuid: fromUuid
        jobType: jobType
        messageType: messageType
        responseId: uuid.v4()

    @jobManager.createRequest 'request', request, callback

  _doCallback: (request, code, callback) =>
    response =
      metadata:
        responseId: request.metadata.responseId
        code: code
        status: http.STATUS_CODES[code]
    callback null, response

  do: (request, callback) =>
    {toUuid, fromUuid, messageType, jobType} = request.metadata
    message = JSON.parse request.rawData

    @uuidAliasResolver.resolve toUuid, (error, toUuid) =>
      return callback error if error?
      @uuidAliasResolver.resolve fromUuid, (error, fromUuid) =>
        return callback error if error?
        @_send {toUuid, fromUuid, messageType, message, jobType}, (error) =>
          return callback error if error?
          return @_doCallback request, 204, callback

  _send: ({toUuid, fromUuid, messageType, message, jobType}, callback=->) =>
    @subscriptionManager.emitterListForType {emitterUuid: toUuid, type: messageType}, (error, subscriptions) =>
      return callback error if error?
      options = {toUuid, fromUuid, messageType, message, jobType}
      async.eachSeries subscriptions, async.apply(@_publishSubscription, options), callback

  _publishSubscription: ({toUuid, fromUuid, messageType, message, jobType}, {subscriberUuid}, callback) =>
    @uuidAliasResolver.resolve subscriberUuid, (error, subscriberUuid) =>
      return callback error if error?
      if messageType == "received"
        return callback() unless subscriberUuid == toUuid

      options = {uuid: subscriberUuid, expireSeconds: 86400} # 86400 == 24 hours
      auth =
        uuid: subscriberUuid

      message = JSON.parse JSON.stringify(message)

      message.forwardedFor ?= []

      @uuidAliasResolver.resolve toUuid, (error, resolvedToUuid) =>
        # use the real uuid of the device
        message.forwardedFor.push resolvedToUuid

        @_createJob {toUuid: subscriberUuid, fromUuid: subscriberUuid, auth, jobType, messageType, message}, callback

module.exports = DeliverSubscriptions
