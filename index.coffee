return global['caboose-sql'] if global['caboose-sql']?

caboose = Caboose.exports
util = Caboose.util
logger = Caboose.logger

Sequelize = require 'sequelize'
_ = require 'underscore'
_.str = require 'underscore.string'

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
      logger.log "INITIALIZING CABOOSE-SQL"
      
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

  Caboose.app.sequelize = new Sequelize(config.database, config.user, config.password, {
    dialect: config.dialect
    host: config.host
    port: config.port
  })
  

caboose_sql.sqlize = (model_class) ->
  throw new Error('Must define @model') unless model_class.model?

  table_name = model_class.store_in || _.str.underscored(/function +([^\(]+)/.exec(model_class.toString())[1])
  delete model_class.store_in

  instance_methods = _.chain(model_class::).keys().inject((o, k) ->
    o[k] = model_class::[k]
    o
  , {}).value()

  Object.defineProperty(model_class, '__model__', value: Caboose.app.sequelize.define(table_name, model_class.model, {instanceMethods: instance_methods}))
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

caboose_sql.Queryable = {
  where: (query) ->
    new Query(@__model__, query)
}

caboose_sql[f] = Sequelize[f] for f in ['STRING', 'TEXT', 'INTEGER', 'DATE', 'BOOLEAN', 'FLOAT']

module.exports = global['caboose-sql'] = caboose_sql
