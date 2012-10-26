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


  if config.cache?.enabled
    throw new Error("Caching only supports redis at the moment") unless config.cache.redis?
    try
      redis = require 'redis'
    catch e
      throw new Error("You need to npm install redis for caching to function")

    redis_url_config = require('url').parse(config.cache.redis.url) if config.cache.redis.url?
    [redis_url_username, redis_url_password] = redis_url_config.auth.split(':') if redis_url_config?.auth?
    redis_host = redis_url_config?.hostname || config.cache.redis.host || 'localhost'
    redis_port = redis_url_config?.port || config.cache.redis.port || 6379
    redis_database = redis_url_config?.pathname.slice(1) || config.cache.redis.database
    redis_password = redis_url_password || config.cache.redis.password

    client = redis.createClient(redis_port, redis_host)
    client.auth(redis_password) if redis_password?
    if redis_database?
      throw new Error('Database must be an integer') unless parseInt(redis_database).toString() is redis_database.toString()
      client.select(parseInt(redis_database))

    caboose_sql.cache = {
      client: client
      ttl: config.cache.redis.ttl ? 60
      prefix: config.cache.redis.prefix ? 'sql:'
    }

  Caboose.app.sequelize = new Sequelize(config.database, config.user, config.password, {
    dialect: config.dialect
    host: config.host
    port: config.port
    logging: if Caboose.env is "development" then true else false
  })


caboose_sql.clear_cache = () ->
  return unless caboose_sql.cache?
  caboose_sql.cache.client.keys "sql:*", (err, keys) =>
    caboose_sql.cache.client.del keys, (err, count) =>

caboose_sql.sqlize = (model_class, options={}) ->
  throw new Error('Must define @model') unless model_class.model?

  options.cache ?= {}
  options.cache.ttl ?= caboose_sql.cache?.ttl
  options.cache.prefix ?= caboose_sql.cache?.prefix
  options.cache.enabled ?= false

  table_name = model_class.store_in || _.str.underscored(/function +([^\(]+)/.exec(model_class.toString())[1])
  delete model_class.store_in

  instance_methods = _.chain(model_class::).keys().inject((o, k) ->
    o[k] = model_class::[k]
    o
  , {}).value()

  Object.defineProperty(model_class, '__model__', value: Caboose.app.sequelize.define(table_name, model_class.model, {timestamps: false, instanceMethods: instance_methods}))

  if options.cache.enabled and caboose_sql.cache?
    Object.defineProperty(model_class, '__cache__', value: _.extend({client: caboose_sql.cache.client}, options.cache))
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

  sort: (fields) ->
    @options.order = _(fields).map((direction, key) ->
      dir = switch direction
        when 1 then 'ASC'
        when -1 then 'DESC'
        else 'ASC'
      "#{key} #{dir}"
    ).join(', ')
    @

  fields: (fields) ->
    if typeof fields is 'string'
      fields = [fields]
    unless Array.isArray(fields)
      fields = _(fields).keys()
    @options.attributes = fields
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

  update: (data, callback) ->
    @model.updateAttributes(data).error((err) ->
      callback(err)
    ).success(callback)

CachedQuery = caboose_sql.CachedQuery = class CachedQuery extends Query
  constructor: (@model, @cache, @query) ->
    @options = {}

  __cached_query__: (method, callback) ->
    query_hash = (method, query) =>
      hash = crypto.createHash('md5')
      h = {
        table: @model.tableName,
        method: method
        query: query
      }
      hash.update JSON.stringify(h)
      "#{@cache.prefix}#{hash.digest('hex')}"

    read_cache = (hash, cb) =>
      @cache.client.get(hash, cb)

    write_cache = (hash, result) =>
      # cache only values of result
      if result is null
        values = null
      else if Array.isArray(result)
        values = result.map (r) -> r.values
      else if result.values?
        values = result.values
      else
        # handle count
        values = result

      @cache.client.setex(hash, @cache.ttl, JSON.stringify(values))

    inflate_cached_object = (method, cached_result) =>
      values = JSON.parse(cached_result)
      return values if method is 'count'
      return values.map((v) -> v = @model.build(v); v.isNewRecord = false; v) if Array.isArray(values)
      v = @model.build(values)
      v.isNewRecord = false
      v

    read_database = (hash, cb) =>
      CachedQuery.__super__[method].call @, (err, result) =>
        return callback(err) if err?
        cb(err, result)

        write_cache(hash, result)


    query = @__prepare_query__()
    hash = query_hash(method, query)

    read_cache hash, (err, cached_result) ->
      return callback(err) if err?

      return read_database(hash, callback) unless cached_result?
      try
        callback(null, inflate_cached_object(method, cached_result))
      catch e
        read_database(hash, callback)

  first: (callback) ->
    @__cached_query__('first', callback)

  array: (callback) ->
    @__cached_query__('array', callback)

  count: (callback) ->
    @__cached_query__('count', callback)

caboose_sql.Queryable = {
  where: (query) ->
    if @__cache__? then new CachedQuery(@__model__, @__cache__, query) else new Query(@__model__, query)

  limit: (value) -> @where({}).limit(value)
  skip: (value) -> @where({}).skip(value)
  sort: (fields) -> @where({}).sort(fields)
  fields: (fields) -> @where({}).fields(fields)
  first: (callback) -> @where({}).first(callback)
  array: (callback) -> @where({}).array(callback)
  count: (callback) -> @where({}).count(callback)
}

caboose_sql[f] = Sequelize[f] for f in ['STRING', 'TEXT', 'INTEGER', 'DATE', 'BOOLEAN', 'FLOAT']

module.exports = global['caboose-sql'] = caboose_sql
