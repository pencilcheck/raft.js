var moment = require('moment')

module.exports = function () {
  this.states = []

  this.push = function (snapshot, name, lastChanged) {
    var timestamp = moment(new Date())
    this.states.push({timestamp: timestamp, lastChanged: lastChanged, snapshot: snapshot, name: (name || timestamp.format('MMMM Do YYYY, h:mm:ss a'))})
  }

  this.get = function (name) {
    var result = {}
    this.states.forEach(function (state) {
      if (state.name == name) {
        result = state
      }
    })
    return result
  }

  this.current = function () {
    return this.states[this.states.length-1] ? this.states[this.states.length-1] : {}
  }
}
