_ = require 'lodash'
async = require 'async'
uuid = require 'uuid'
SubscriptionManager = require 'meshblu-core-manager-subscription'
TokenManager = require 'meshblu-core-manager-token'
http = require 'http'

class DeliverSubscriptions
  constructor: (options={},dependencies={}) ->
    {cache,datastore,pepper,uuidAliasResolver,@jobManager} = options
    {@tokenManager} = dependencies
    @subscriptionManager ?= new SubscriptionManager {datastore, uuidAliasResolver}
    @tokenManager ?= new TokenManager {cache, uuidAliasResolver, pepper}

  _createJob: ({messageType, toUuid, message, fromUuid, auth}, callback) =>
    request =
      data: message
      metadata:
        auth: auth
        toUuid: toUuid
        fromUuid: fromUuid
        jobType: 'DeliverMessage'
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
    @tokenManager.generateAndStoreTokenInCache subscriberUuid, (error, token) =>
      auth =
        uuid: subscriberUuid
        token: token
      @_createJob {toUuid: subscriberUuid, fromUuid: subscriberUuid, auth, messageType, message}, callback

module.exports = DeliverSubscriptions
