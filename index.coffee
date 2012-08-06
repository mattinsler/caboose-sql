caboose = Caboose.exports
util = Caboose.util
logger = Caboose.logger

module.exports =
  'caboose-plugin': {
    install: (util, logger) ->
      logger.title 'Running installer for caboose-sql'
    
    initialize: ->
      logger.title 'Initializing caboose-sql'
  }
