/*
 * TODO:
 * 5.1 Basics [DONE]
 * 5.2 Leader election [DONE]
 * 5.3 Log replication [DONE]
 * 5.4 Safety [DONE]
 * 5.4.1, 5.4.2, 5.4.3 [DONE]
 * 5.5 Follower and candidate crashes [DONE]
 * 5.6 Timing and availability [DONE]
 * 6   Cluster membership changes
 * 7.  Log compaction
 * 8.  Client interaction
 * 9.  Persistence (client pending requests + logs)
 */

var q = require('q')

function voteGranted(results) {
  return results.voteGranted ? 1 : 0
}

function overHalf(count, list) {
  return count > list.length / 2
}

function determineTimeout() {
  return Math.floor(Math.random() * 2000 + 1000)
}

function addAll(dest, items) {
  items.forEach(function (item) {
    dest[JSON.stringify(item)] = item
  })
}

function removeAll(dest, items) {
  items.forEach(function (item) {
    delete dest[JSON.stringify(item)]
  })
}

Object.prototype.extend = function (base, obj) {
  var dest = {}
  Object.keys(base).forEach(function (key) {
    if (!dest[key])
      dest[key] = base[key]
  })

  Object.keys(obj).forEach(function (key) {
    if (!dest[key])
      dest[key] = obj[key]
  })

  return dest
}

Object.prototype.values = function (obj) {
  var values = []
  Object.keys(obj).forEach(function (key) {
    values.push(obj[key])
  })
  return values
}

var Configuration = (function () {
  var ConfigurationBase = function (initial) {
    this.currentConfiguration = {}
    this.newConfiguration = {}
    this.cloned = false

    addAll(this.currentConfiguration, initial)

    this.addServers = function (servers) {
      if (!this.cloned) {
        addAll(this.newConfiguration, Object.values(this.currentConfiguration))
        this.cloned = true
      }
      addAll(this.newConfiguration, servers)
    }

    this.removeServers = function (servers) {
      if (!this.cloned) {
        addAll(this.newConfiguration, Object.values(this.currentConfiguration))
        this.cloned = true
      }
      removeAll(this.newConfiguration, servers)
    }

    this.setServers = function (servers) {
      this.newConfiguration = {}
      addAll(this.newConfiguation, servers)
      this.cloned = false
    }

    this.servers = function () {
      return Object.values(Object.extend(this.currentConfiguration, this.newConfiguration))
    }

    this.commit = function () {
      this.currentConfiguration = this.newConfiguration
      this.newConfiguration = {}
    }
  }

  return ConfigurationBase
})()

module.exports = function (socket, ip, initial, sm) {
  // Persistent state on all servers
  this.role = 'follower' // [follower, candidate, leader]
  this.currentTerm = 0
  this.votes = 0
  this.log = [] // Three states: served -> executed
  this.socket = socket
  this.configuration = new Configuration(initial) // Configuration
  this.serverId = null // Current server id
  this.leaderId = null
  this.voteFor = {}
  this.electionTimeout = determineTimeout()
  this.ip = ip
  this.id = this.configuration.servers().find(function (server) {
    return server.ip == ip
  }).id
  this.sm = sm // State machine

  // Volatile state on all servers
  this.commitIndex = 0
  this.lastApplied = 0
  this.timeSinceLastHeartbeatFromLeader = 0 // in ms

  // Volatile state on leaders
  this.nextIndex = {}
  this.matchIndex = {}

  var self = this
  this.configuration.servers().forEach(function (server) {
    self.nextIndex[server.id] = self.log.length
    self.matchIndex[server.id] = 0
  })

  this.multicast2 = function (func, count, increment, resolve) {
    var self = this,
        dfd = q.defer()
        neighborList = this.configuration.servers().filter(function (server) {return server.id != this.id}, this),
        qList = []

    count = count || 0
    increment = increment || function (count) {
      return 1
    }
    resolve = resolve || function (count, list) {
      return count == list
    }

    neighborList.forEach(function (server) {
      qList.push(func.call(self, server.id).then(increment).then(function (yes) {
        count += yes
        console.log('count is ' + count)
        
        if (resolve(count, neighborList) && dfd) {
          dfd.resolve()
          dfd = null
        }
      }))
    })

    q.all(qList).then(function () {
      console.log('q.all')
      if (dfd)
        dfd.reject()
    })

    return dfd.promise
  }

  this.multicast = function (cb) {
    return q.all(this.configuration.servers().filter(function (server) {
      return server.id != this.id
    }, this).map(cb))
  }

  this.ackTimeout = 1000 // ms
  this.waitingAcks = {}
  this.messageIndex = 0
  this.rpc = function () {
    var destId = arguments[0],
        args = Array.prototype.slice.call(arguments, 0).slice(2)[0]
    args.func = arguments[1]

    return this.send(destId, args)
  }

  this.ack = function (destId, messageIndex, payload) {
    console.log('ack from ' + this.id + ' to ' + destId)
    return this.send(destId, payload, 'ack', messageIndex)
  }

  this.send = function (dest, payload, type, messageIndex) {
    var self = this,
        message = {
          type: type || 'message',
          src_id: this.id,
          dest_id: dest,
          messageIndex: messageIndex || (this.messageIndex += 1),
          payload: payload
        }

    console.log('sending ' + message.payload.func + ' (' + message.type + ') to ' + dest + ' with index ' + message.messageIndex)
    this.socket.send(JSON.stringify(message))

    if (type != 'ack') {
      var dfd = q.defer()
      var ackId = parseInt(message.src_id).toString() + parseInt(message.dest_id).toString() + parseInt(message.messageIndex).toString()
      console.log('storing ack id at ' + ackId)
      this.waitingAcks[ackId] = dfd
      setInterval(function () {
        // Resend indefinitely if not receive ack in timeout
        self.socket.send(JSON.stringify(message))
      }, this.ackTimeout)
      return dfd.promise
    }
  }

  this.isUpToDate = function (term, lastLogIndex, lastLogTerm) {
    if (term > this.currentTerm)
      this.role = 'follower'
    
    if (lastLogIndex && lastLogTerm) {
      if (this.log[lastLogIndex].term == args.lastLogTerm) {
        return this.log.length-1 < lastLogIndex
      }
      return this.log[lastLogIndex].term < lastLogTerm
    } else {
      return this.currentTerm <= term
    }
  }

  this.eventLoop = function () {
    var self = this
    setInterval(function () {
      if (self.role == 'follower' || self.role == 'candidate') {
        self.timeSinceLastHeartbeatFromLeader += 1

        // So weird, assigning to a variable works
        var time = self.timeSinceLastHeartbeatFromLeader
        var timeout = self.electionTimeout
        if (time > timeout) {
          console.log('[ELECTION] start election (' + time + ' > ' + timeout + ')' + ' role => ' + self.role + ' leader => ' + self.leaderId)
          // Restart election
          self.electionTimeout = determineTimeout()
          self.currentTerm += 1
          self.role = 'candidate'
          self.votes = 1 // Vote for self
          self.voteFor[self.currentTerm] = self.id
          self.leaderId = null
          self.timeSinceLastHeartbeatFromLeader = 0

          self.multicast2(self.requestVote, self.votes, voteGranted, overHalf).then(function () {
            console.log('I won')
            self.role = 'leader'
            self.leaderId = self.id

            self.configuration.servers().forEach(function (server) {
              self.nextIndex[server.id] = self.log.length
              self.matchIndex[server.id] = 0
            })

            self.multicast2(self.heartbeat)
          }, function () {
            console.log('I lost')
          })
        }
      }

      function updateEntries(serverId) {
        var index = self.nextIndex[serverId],
            entries = self.log.slice(index, index+1)
        self._appendEntries(entries, serverId).then(function (results) {
          if (!results.success) {
            // Update term??
            self.nextIndex[serverId] -= 1
            updateEntries(serverId)
          } else {
            self.matchIndex[serverId] = index
            self.nextIndex[serverId] += 1
          }
        })
      }

      if (self.role == 'leader') {
        Object.keys(self.nextIndex).forEach(function (serverId) {
          if (self.log.length-1 >= self.nextIndex[serverId])
            updateEntries(serverId)
        })
      }
    })

    setInterval(function () {
      if (self.role == 'leader') {
        self.multicast2(self.heartbeat)
      }
    }, self.electionTimeout)
  }

  // Commitment (also means persistence)
  this.commit = function (index) {
    var command = this.log[index][0],
        data    = this.log[index][1]
    this.sm[command].apply(this, data)
    this.log[index].state = 'executed'
    this.commitIndex = this.commitIndex < index ? index : this.commitIndex
    this.lastApplied = index
  }

  this.leader = function () {
    this.configuration.servers().find(function (server) {
      return server.id == this.leaderId
    })
  }

  this.start = function () {
    var self = this
    this.socket.onmessage = function (event) {
      var data = JSON.parse(event.data)
      if (data.type == 'onopen') {
        self.serverId = data.payload
        self.socket.send(JSON.stringify({type: 'setup', payload: [self.serverId, self.id]}))
        self.eventLoop()
      } else if (data.type == 'ack') {
        console.log('[ACK] from ' + data.src_id)
        var ackId = parseInt(data.dest_id).toString() + parseInt(data.src_id).toString() + parseInt(data.messageIndex).toString()
        var promise = self.waitingAcks[ackId]
        if (promise) {
          console.log('found the promise at ' + ackId)
          promise.resolve(data.payload)
        }
      } else {
        self.ack(data.src_id, data.messageIndex, self.respondToRequest(data.payload))
      }
    }

    this.socket.onerror = function (event) {
      console.log('onerror', event)
    }

    this.socket.onclose = function (event) {
      console.log('onclose', event)
    }
  }

  // Only invoked by leader when serving client requests
  this.serve = function (command, data) {
    var self = this

    var requestIndex = self.log.length
    self.log.push({command: [command, data], term: this.currentTerm, state: 'served'})

    function appended(results) {
      if (results.success) {
        self.matchIndex[server.id] = index
        self.nextIndex[server.id] += 1
      }
    }

    return self.multicast(function (server) {
      var index = this.nextIndex[server.id],
          entries = this.log.slice(index, index+1)

      return self._appendEntries(entries, server.id).then(function (results) {
        if (!results.success) {
          // Update term??
        } else {
          self.matchIndex[server.id] = index
          self.nextIndex[server.id] += 1
        }
      })
    }).then(function () {
      // Replicated to all
      self.commit(requestIndex)
    })
  }

  // Only invoked by leader
  this._appendEntries = function (entries, destId) {
    return this.rpc(destId, 'appendEntries', {
      term: this.currentTerm,
      leaderId: this.leaderId,
      prevLogIndex: this.log.length-2 >= 0 ? this.log.length-2 : null,
      prevLogTerm: this.log.length-2 >= 0 ? this.log[this.log.length-2].term : null,
      entries: entries || [], // For now entries contain only one entry
      leaderCommit: this.commitIndex,
    })
  }

  this.appendEntries = function (entries) {
    var self = this
    return function (serverId) {
      return self._appendEntries(entries, serverId)
    }
  }

  this.heartbeat = function () {
    return this.appendEntries([]).apply(this, arguments)
  }

  // Only invoked by candidate
  this.requestVote = function (destId) {
    return this.rpc(destId, 'requestVote', {
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex: this.log.length-1,
      lastLogTerm: this.log[this.log.length-1] && this.log[this.log.length-1].term || 0
    })
  }

  this.respondToRequest = function (args) {
    var role = this.role
    switch(args.func) {
      case 'appendEntries':
        if (!this.isUpToDate(args.term)) {
          return {func: args.func, term: this.currentTerm, success: false}
        }

        this.role = 'follower'
        this.timeSinceLastHeartbeatFromLeader = 0

        this.leaderId = args.leaderId
        this.commitIndex = args.commitIndex > this.commitIndex ? Math.min(args.commitIndex, this.log.length-1) : this.commitIndex

        var newLogIndex = args.prevLogIndex+1
        if (this.log[newLogIndex] && this.log[newLogIndex].term != args.entries[0].term) {
          this.log = this.log.slice(0, newLogIndex)
        }

        this.log.concat(args.entries)

        return {func: args.func, term: this.currentTerm, success: this.log[args.prevLogIndex] ? this.log[args.prevLogIndex].term == args.prevLogTerm : true}
        break
      case 'requestVote':
        console.log('[RPC] requestVote from ' + args.candidateId)
        if (this.isUpToDate(args.term, args.lastLogIndex, args.lastLogTerm)) {
          if (this.voteFor[args.term]) {
            console.log('vote rejected because already voted')
            return {func: args.func, term: this.currentTerm, voteGranted: false}
          }

          console.log('vote granted because it is up to date')
          return {func: args.func, term: this.currentTerm, voteGranted: true}
        } else {
          console.log('vote rejected because it is not up to date')
          return {func: args.func, term: this.currentTerm, voteGranted: false}
        }
        break
    }
  }
}
