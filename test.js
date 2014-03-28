module.exports = function (socket) {
  this.socket = socket
  this.count = 0
  this.limit = Math.floor(Math.random() * 50)
  this._loop = function () {
    var self = this
    setInterval(function () {
      self.count += 1
      console.log(self.count, self.limit)
      if (self.count > self.limit) {
        self.limit = Math.floor(Math.random() * 50)
        console.log('over')
      } else
        console.log('under')
    })
  }

  this.start = function () {
    var self = this
    self._loop()
  }
}

