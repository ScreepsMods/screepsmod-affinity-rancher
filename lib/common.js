const EventEmitter = require('events').EventEmitter
module.exports = function (config) {
  config.affinity = new EventEmitter()
  Object.assign(config.affinity, {})
}
