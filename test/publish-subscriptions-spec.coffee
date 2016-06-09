_ = require 'lodash'
uuid = require 'uuid'
redis = require 'fakeredis'
mongojs = require 'mongojs'
Datastore = require 'meshblu-core-datastore'
Cache = require 'meshblu-core-cache'
JobManager = require 'meshblu-core-job-manager'
DeliverSubscriptions = require '../'

describe 'DeliverSubscriptions', ->
  beforeEach (done) ->
    database = mongojs 'subscription-test', ['subscriptions']
    @datastore = new Datastore
      database: database
      collection: 'subscriptions'

    database.subscriptions.remove done

  beforeEach ->
    @redisKey = uuid.v1()
    @redisPubSubKey = uuid.v1()
    @pepper = 'im-a-pepper'
    @uuidAliasResolver = resolve: (uuid, callback) => callback(null, uuid)
    @firehoseClient = new Cache
      client: _.bindAll redis.createClient @redisPubSubKey
      namespace: 'meshblu-token-one-time'

    @jobManager = new JobManager
      client: _.bindAll redis.createClient @redisKey
      timeoutSeconds: 1

    options = {
      @firehoseClient
      @datastore
      @jobManager
      @uuidAliasResolver
    }

    @client = _.bindAll redis.createClient @redisPubSubKey

    @sut = new DeliverSubscriptions options

  describe '->do', ->
    context 'when subscriptions exist', ->
      beforeEach (done) ->
        @client.subscribe 'sent:subscriber-uuid', done

      beforeEach ->
        @client.once 'message', (error, @message) =>

      beforeEach (done) ->
        record =
          type: 'sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'subscriber-uuid'

        @datastore.insert record, done

      beforeEach (done) ->
        request =
          metadata:
            responseId: 'its-electric'
            toUuid: 'emitter-uuid'
            fromUuid: 'someone-uuid'
            messageType: 'sent'
            jobType: 'DeliverSentMessage'
          rawData: '{"devices":"*"}'

        @sut.do request, (error, @response) => done error

      it 'should return a 204', ->
        expectedResponse =
          metadata:
            responseId: 'its-electric'
            code: 204
            status: 'No Content'

        expect(@response).to.deep.equal expectedResponse

      describe 'JobManager gets DeliverMessage job', (done) ->
        beforeEach (done) ->
          @jobManager.getRequest ['request'], (error, @request) =>
            done error

        it 'should be a sent messageType', ->
          auth =
            uuid: 'subscriber-uuid'

          {rawData, metadata} = @request
          expect(metadata.auth).to.deep.equal auth
          expect(metadata.jobType).to.equal 'DeliverSentMessage'
          expect(metadata.messageType).to.equal 'sent'
          expect(metadata.toUuid).to.equal 'subscriber-uuid'
          expect(metadata.fromUuid).to.equal 'subscriber-uuid'
          expect(rawData).to.equal JSON.stringify devices:'*', forwardedFor:['emitter-uuid']

    context 'when there is a received subscription for someone besides yourself', ->
      beforeEach (done) ->
        @client.subscribe 'received:subscriber-uuid', done

      beforeEach ->
        @client.once 'message', (error, @message) =>

      beforeEach (done) ->
        record =
          type: 'received'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'subscriber-uuid'

        @datastore.insert record, done

      beforeEach (done) ->
        request =
          metadata:
            responseId: 'its-electric'
            toUuid: 'emitter-uuid'
            fromUuid: 'someone-uuid'
            messageType: 'received'
            jobType: 'DeliverReceivedMessage'
          rawData: '{"devices":"*"}'

        @sut.do request, (error, @response) => done error

      describe 'JobManager should not get a DeliverMessage job', (done) ->
        beforeEach (done) ->
          @jobManager.getRequest ['request'], (error, @request) =>
            done error

        it "shouldn't add the job", ->
          expect(@request).to.not.exist

    context 'when there is a received subscription for someone allowed to configure the device', ->
      beforeEach (done) ->
        @client.subscribe 'received:subscriber-uuid', done

      beforeEach ->
        @client.once 'message', (error, @message) =>

      beforeEach (done) ->
        record =
          type: 'received'
          emitterUuid: 'subscriber-uuid'
          subscriberUuid: 'subscriber-uuid'

        @datastore.insert record, done

      beforeEach (done) ->
        request =
          metadata:
            responseId: 'its-electric'
            toUuid: 'subscriber-uuid'
            fromUuid: 'someone-uuid'
            messageType: 'received'
            jobType: 'DeliverReceivedMessage'
          rawData: '{"devices":"*"}'

        @sut.do request, (error, @response) => done error


      it 'should return a 204', ->
        expectedResponse =
          metadata:
            responseId: 'its-electric'
            code: 204
            status: 'No Content'

        expect(@response).to.deep.equal expectedResponse

      describe 'JobManager gets DeliverMessage job', (done) ->
        beforeEach (done) ->
          @jobManager.getRequest ['request'], (error, @request) => done error

        it 'should be a received messageType', ->
          auth =
            uuid: 'subscriber-uuid'

          {rawData, metadata} = @request
          expect(metadata.auth).to.deep.equal auth
          expect(metadata.jobType).to.equal 'DeliverReceivedMessage'
          expect(metadata.messageType).to.equal 'received'
          expect(metadata.toUuid).to.equal 'subscriber-uuid'
          expect(metadata.fromUuid).to.equal 'subscriber-uuid'
          expect(rawData).to.equal JSON.stringify devices:'*', forwardedFor:['subscriber-uuid']
