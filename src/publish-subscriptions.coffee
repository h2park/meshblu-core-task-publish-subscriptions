_ = require 'lodash'
async = require 'async'
uuid = require 'uuid'
SubscriptionManager = require 'meshblu-core-manager-subscriptions'
http = require 'http'

class DeliverSubscriptions
  constructor: (options={},dependencies={}) ->
    {@cache,datastore,pepper,uuidAliasResolver,@jobManager} = options
    @subscriptionManager ?= new SubscriptionManager {datastore, uuidAliasResolver}

  _doCallback: (request, code, callback) =>
    response =
      metadata:
        responseId: request.metadata.responseId
        code: code
        status: http.STATUS_CODES[code]
    callback null, response

  do: (request, callback) =>
    {toUuid, fromUuid, messageType} = request.metadata
    message = JSON.parse request.rawData

    @_send {toUuid, messageType, message}, (error) =>
      return callback error if error?
      return @_doCallback request, 204, callback

  _send: ({toUuid,messageType,message}, callback=->) =>
    @subscriptionManager.emitterListForType {emitterUuid: toUuid, type: messageType}, (error, subscriptions) =>
      return callback error if error?
      async.eachSeries subscriptions, async.apply(@_publishSubscription, {toUuid,messageType,message}), callback

  _publishSubscription: ({toUuid,messageType,message}, {subscriberUuid}, callback) =>
    @cache.publish "#{messageType}:#{subscriberUuid}", message, callback

module.exports = DeliverSubscriptions
