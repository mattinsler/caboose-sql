return global['caboose-sql'] if global['caboose-sql']?

caboose = Caboose.exports
util = Caboose.util
logger = Caboose.logger

Sequelize = require 'sequelize'
_ = require 'underscore'
_.str = require 'underscore.string'
crypto = require 'crypto'

caboose_sql = module.exports =
  'caboose-plugin': {
    install: (util, logger) ->
      util.mkdir(Caboose.path.app.join('models'))
      util.create_file(
        Caboose.path.config.join('caboose-sql.json'),
        JSON.stringify({
          dialect: 'postgres'
          user: 'username'
          password: 'password'
          host: 'localhost'
          port: 5432
          database: Caboose.app.name
        }, null, 2)
      )

    initialize: ->
      if Caboose?.app?.config?['caboose-sql']?
        caboose_sql.configure(Caboose.app.config['caboose-sql'])
  }

caboose_sql.configure = (config) ->
  if config.url
    uri = require('url').parse(config.url)
    config.host = uri.hostname
    config.port = parseInt(uri.port) if uri.port?
    config.database = uri.pathname.replace /^\//g, ''
    [config.user, config.password] = uri.auth.split(':') if uri.auth?


  if config.cache?
    throw new Error("Caching only supports redis at the moment") unless config.cache.redis?
    try
      redis = require 'node-redis'
    catch e
      throw new Error("You need to install node-redis for caching to function")

    redis_url_config = require('url').parse(config.cache.redis.url) if config.cache.redis.url?
    [redis_url_username, redis_url_password] = redis_url_config.auth.split(':') if redis_url_config?.auth?
    redis_host = redis_url_config?.hostname || config.cache.redis.host || 'localhost'
    redis_port = redis_url_config?.port || config.cache.redis.port || 6379
    redis_database = redis_url_config?.pathname.slice(1) || config.cache.redis.database
    redis_username = redis_url_username || config.cache.redis.username
    redis_password = redis_url_password || config.cache.redis.password

    caboose_sql.cache = redis.createClient(redis_port, redis_host)
    caboose_sql.cache.auth(redis_password) if redis_password?
    caboose_sql.cache_ttl = config.cache.redis.ttl || 60

  Caboose.app.sequelize = new Sequelize(config.database, config.user, config.password, {
    dialect: config.dialect
    host: config.host
    port: config.port
  })


caboose_sql.sqlize = (model_class, options={}) ->
  throw new Error('Must define @model') unless model_class.model?

  options.cache ?= {}
  options.cache.ttl ?= caboose_sql.cache_ttl
  options.cache.enabled ?= false

  table_name = model_class.store_in || _.str.underscored(/function +([^\(]+)/.exec(model_class.toString())[1])
  delete model_class.store_in

  instance_methods = _.chain(model_class::).keys().inject((o, k) ->
    o[k] = model_class::[k]
    o
  , {}).value()

  Object.defineProperty(model_class, '__model__', value: Caboose.app.sequelize.define(table_name, model_class.model, {timestamps: false, instanceMethods: instance_methods}))

  if options.cache.enabled?
    console.log "Setting cache object"
    Object.defineProperty(model_class, '__cache__', value: {client: caboose_sql.cache, ttl: options.cache.ttl})
  delete model_class.model

  _.extend(model_class, caboose_sql.Queryable)

  model_class

Query = caboose_sql.Query = class Query
  constructor: (@model, @query) ->
    @options = {}

  __prepare_query__: ->
    query = {}
    query.where = @query if @query? and Object.keys(@query).length > 0
    _.extend(query, @options)

  limit: (value) ->
    @options.limit = value
    @

  skip: (value) ->
    @options.offset = value
    @

  first: (callback) ->
    @model.find(@__prepare_query__()).error((err) ->
      callback(err)
    ).success((value) =>
      callback(null, value)
    )

  array: (callback) ->
    @model.findAll(@__prepare_query__()).error((err) ->
      callback(err)
    ).success((value) ->
      callback(null, value)
    )

  count: (callback) ->
    @model.count(@__prepare_query__()).error((err) ->
      callback(err)
    ).success((value) ->
      callback(null, value)
    )

CachedQuery = caboose_sql.CachedQuery = class CachedQuery extends Query
  constructor: (@model, @cache, @query) ->
    @options = {}

  query_hash: ->
    hash = crypto.createHash('md5')
    h = {
      table: @model.tableName,
      query: @__prepare_query__()
    }
    hash.update JSON.stringify(h)
    return "sql:#{hash.digest('hex')}"

  cached_query: (method, callback) ->
    h = @query_hash()

    @cache.client.get h, (err, cached_result) =>
      return callback(err) if err?

      try
        return callback null, JSON.parse cached_result
      catch e
        @model[method](@__prepare_query__()).error((err) ->
          callback(err) if err
        ).success((value) =>
          callback(null, value)
          if Array.isArray(value)
            value = value.map (v) -> v.values
          else
            value = value.values
          @cache.client.set h, JSON.stringify(value), (err) =>
            @cache.client.expire h, @cache.ttl, () -> return
        )

  first: (callback) ->
    @cached_query 'find', callback

  array: (callback) ->
    @cached_query 'findAll', callback

  count: (callback) ->
    @cached_query 'count', callback

caboose_sql.Queryable = {
  where: (query) ->
    unless @__cache__?
      return new Query(@__model__, query)
    return new CachedQuery(@__model__, @__cache__, query)
}

caboose_sql[f] = Sequelize[f] for f in ['STRING', 'TEXT', 'INTEGER', 'DATE', 'BOOLEAN', 'FLOAT']

module.exports = global['caboose-sql'] = caboose_sql
