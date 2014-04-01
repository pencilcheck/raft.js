var moment = require('moment')

module.exports = function () {
  this.states = []

  // state should contains name, snapshot, logs
  this.push = function (state) {
    var timestamp = moment(new Date())
    state.name = state.name || timestamp.format('MMMM Do YYYY, h:mm:ss a')
    state.addedAt = timestamp
    state.revision = this.states.length
    this.states.push(state)
  }

  this.get = function (startRevision) {
    return this.states.slice(startRevision || this.states.length-1)
  }
}
