/*
 * TODO:
 * 5.1 Basics [DONE]
 * 5.2 Leader election [DONE]
 * 5.3 Log replication [DONE]
 * 5.4 Safety [DONE]
 * 5.4.1, 5.4.2, 5.4.3 [DONE]
 * 5.5 Follower and candidate crashes [DONE]
 * 5.6 Timing and availability [DONE]
 * 6   Cluster membership changes [DONE]
 * 7.  Log compaction
 * 8.  Client interaction
 * 9.  Persistence (client pending requests + logs)
 * 10. Test all
 */

var q = require('q')

function voteGranted(results) {
  return results.voteGranted ? 1 : 0
}

function overHalf(count, list) {
  return count > list.length / 2
}

function determineTimeout() {
  return Math.floor(Math.random() * 5000 + 4000)
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

Object.defineProperty(Array, 'copy', {
  get: function () {
    return function (array) {
      return array.map(Array.apply.bind(Array, null))
    }
  }
})

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

    this.fromObject = function (obj) {
      if (obj.currentConfiguration)
        this.currentConfiguration = obj.currentConfiguration
      if (obj.newConfiguration)
        this.newConfiguration = obj.newConfiguration
    }

    this.toObject = function () {
      return {currentConfiguration: this.currentConfiguration, newConfiguration: this.newConfiguration}
    }

    this.commit = function () {
      this.currentConfiguration = this.newConfiguration
      this.newConfiguration = {}
    }

    if (Array.isArray(initial)) {
      addAll(this.currentConfiguration, initial)
    } else {
      this.fromObject(initial)
    }
  }

  return ConfigurationBase
})()

function latestConfiguration(log) {
  Array.copy(log).reverse().forEach(function (entry) {
    if (entry.command.startsWith('configuration')) {
      return entry.data
    }
  })

  return false
}

module.exports = function (socket, ip, initial, sm) {
  // Persistent state on all servers
  this.role = 'follower' // [follower, candidate, leader]
  this.currentTerm = 0
  this.log = [] // Three states: served -> executed, TODO: should be loaded from persistence (for now firebase)
  this.socket = socket
  this.configuration = new Configuration(latestConfiguration(this.log) || initial) // Configuration
  this.serverId = null // Current server id
  this.leaderId = null
  this.voteFor = {}
  this.electionTimeout = determineTimeout()
  this.id = ip
  this.sm = sm // State machine

  // Volatile state on all servers
  this.commitIndex = 0
  this.lastApplied = 0
  this.timeSinceLastHeartbeatFromLeader = 0 // in ms

  // Volatile state on leaders
  this.nextIndex = {}
  this.matchIndex = {}

  this.setupIndexes = function () {
    var self = this
    this.configuration.servers().forEach(function (server) {
      self.nextIndex[server] = self.nextIndex[server] || self.log.length
      self.matchIndex[server] = self.matchIndex[server] || 0
    })
  }
  this.setupIndexes()

  this.replicationList = function () {
    var self = this
    // Count leader as part of majority if it is in configuration
    return this.configuration.servers()
  }

  this.votingList = function () {
    var self = this
    // Don't count members not up to date as voting members
    return this.configuration.servers().filter(function (server) {
      if (self.log[self.matchIndex[server]])
        return self.isUpToDate(self.log[self.matchIndex[server]].term, self.matchIndex[server], self.log[self.matchIndex[server]].term)
      else
        return true
    })
  }

  this.multicast = function (func, count, increment, resolve, neighborList) {
    var self = this,
        dfd = q.defer()
        qList = []

    neighborList = neighborList || self.replicationList()
    count = count || 0
    increment = increment || function (count) {
      return 1
    }
    resolve = resolve || function (count, list) {
      return count == list
    }

    function finalize(yes) {
      count += yes
      console.log('count is ' + count)
      
      if (resolve(count, neighborList) && dfd) {
        dfd.resolve()
        dfd = null
      }
    }

    neighborList.forEach(function (server) {
      if (server == this.id) {
        qList.push(q.when(1).then(finalize))
      } else {
        qList.push(func.call(self, server).then(function (results) {
          results.serverId = server
          return results
        }).then(increment).then(finalize))
      }
    })

    q.all(qList).then(function () {
      console.log('q.all')
      if (dfd)
        dfd.reject()
    })

    return dfd.promise
  }

  this.replicateToOne = function (entry, prevLogIndex, destId) {
    return self._appendEntries([entry], prevLogIndex, destId).then(function () {
      if (!results.success) {
        // FIXME: Update term??
        self.nextIndex[destId] -= 1
        self.replicateToOne(entry, prevLogIndex, destId)
      } else {
        self.matchIndex[destId] = prevLogIndex + 1
        self.nextIndex[destId] += 1
      }
    })
  }

  this.requestVoteFromMajority = function () {
    var votes = 1
    return this.multicast(this.requestVote, votes, voteGranted, overHalf, this.votingList())
  }

  this.replicateToMajority = function (entries, prevLogIndex) {
    var self = this
    function appended(results) {
      if (results.success) {
        self.matchIndex[results.serverId] = prevLogIndex + entries.length
        self.nextIndex[results.serverId] += entries.length
      }
      return results.success ? 1 : 0
    }

    return self.multicast(self.appendEntries(entries, prevLogIndex), null, appended, overHalf)
  }

  // FIXME: Better if it is implemented with ES6 generators once it has shim
  this.upToDateWithMajority = function () {
    function upToDate(results) {
      return results.success ? 1 : 0
    }
    return this.multicast(this.isUpToDateRPC, null, upToDate, overHalf)
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
      var ackId = message.src_id.toString() + message.dest_id.toString() + message.messageIndex.toString()
      console.log('storing ack id at ' + ackId)
      this.waitingAcks[ackId] = dfd
      setInterval(function () {
        // Resend indefinitely if not receive ack in timeout
        self.socket.send(JSON.stringify(message))
      }, this.ackTimeout)
      return dfd.promise
    }
  }

  // Invoked by candidate
  this.isUpToDateRPC = function (destId) {
    return this.rpc(destId, 'isUpToDate', {
      term: this.currentTerm,
      lastLogIndex: this.log.length-1 > -1 ? this.log.length-1 : null,
      lastLogTerm: this.log.length-1 > -1 ? this.log[this.log.length-1].term : null
    })
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
      // Can only requestVote if it is included in the current configuration
      if ((self.role == 'follower' || self.role == 'candidate') 
          && self.configuration.servers().indexOf(self.id) > -1) {
        self.timeSinceLastHeartbeatFromLeader += 1

        // So weird, assigning to a variable works
        var time = self.timeSinceLastHeartbeatFromLeader
        var timeout = self.electionTimeout
        if (time > timeout) {
          self.upToDateWithMajority().then(function () {
            console.log('[ELECTION] start election (' + time + ' > ' + timeout + ')' + ' role => ' + self.role + ' leader => ' + self.leaderId)
            // Restart election
            self.electionTimeout = determineTimeout()
            self.currentTerm += 1
            self.role = 'candidate'
            self.voteFor[self.currentTerm] = self.id
            self.leaderId = null
            self.timeSinceLastHeartbeatFromLeader = 0

            self.requestVoteFromMajority().then(function () {
              console.log('I won')
              self.role = 'leader'
              self.leaderId = self.id

              // Reset indexes
              self.configuration.servers().forEach(function (server) {
                self.nextIndex[server] = self.log.length
                self.matchIndex[server] = 0
              })

              self.multicast(self.heartbeat)
            }, function () {
              console.log('I lost')
            })
          }, function () {
            self.role = 'follower'
          })
        }
      }

      if (self.role == 'leader') {
        Object.keys(self.nextIndex).forEach(function (serverId) {
          if (self.log.length-1 >= self.nextIndex[serverId])
            self.replicateToOne(self.log[self.nextIndex[serverId]], self.nextIndex[serverId]-1, serverId)
        })
      }
    })

    setInterval(function () {
      if (self.role == 'leader') {
        self.multicast(self.heartbeat)
      }
    }, self.electionTimeout)
  }

  // TODO: Commit to STM (also persistence)
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
      return server == this.leaderId
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
        var ackId = data.dest_id.toString() + data.src_id.toString() + data.messageIndex.toString()
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
    var self = this,
        prevLogIndex = self.log.length-1,
        entries = []

    if (command.startsWith('configuration')) {
      // Initiate 2-phase configuration change
      // Replicate the joint configuration
      self.configuration[data[0]].apply(self, data.slice(1))
      self.setupIndexes()
      entries = [{
        command: 'configuration',
        data: self.configuration.toObject(),
        term: self.currentTerm,
        state: 'executed'
      }]
      self.log.concat(entries)
      self.replicateToMajority(entries, prevLogIndex)
        .then(function () {
          // Now replicate the new configuration
          self.configuration.commit()
          self.setupIndexes()
          entries = [{
            command: 'configuration',
            data: self.configuration.toObject(),
            term: self.currentTerm,
            state: 'executed'
          }]
          prevLogIndex = self.log.length-1
          self.log.concat(entries)
          self.replicateToMajority(entries, prevLogIndex).then(function () {
            // Step down if the new configuration does not include itself
            if (self.configuration.servers().indexOf(self.id) < 0) {
              self.role = 'follower'
            }
          })
        })
    } else {
      entries = [{command: [command, data], term: self.currentTerm, state: 'served'}]
      self.log.concat(entries)
      return self.replicateToMajority(entries, prevLogIndex)
        .then(function () {
          self.commit(entries, prevLogIndex+1)
        })
    }
  }

  // Only invoked by leader
  this._appendEntries = function (entries, prevLogIndex, destId) {
    return this.rpc(destId, 'appendEntries', {
      term: this.currentTerm,
      leaderId: this.leaderId,
      prevLogIndex: prevLogIndex >= 0 ? prevLogIndex : null,
      prevLogTerm: prevLogIndex >= 0 ? this.log[prevLogIndex].term : null,
      entries: entries || [], // For now entries should only contain one entry
      leaderCommit: this.commitIndex,
    })
  }

  this.appendEntries = function (entries, prevLogIndex) {
    var self = this
    return function (serverId) {
      return self._appendEntries(entries, prevLogIndex, serverId)
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
      case 'isUpToDate':
        return {
          func: args.func,
          term: this.currentTerm,
          success: this.isUpToDate(args.term, args.lastLogIndex, args.lastLogTerm)
        }
        break
      case 'appendEntries':
        if (!this.isUpToDate(args.term)) {
          return {func: args.func, term: this.currentTerm, success: false}
        }

        this.role = 'follower'
        this.timeSinceLastHeartbeatFromLeader = 0

        this.leaderId = args.leaderId
        this.commitIndex = args.leaderCommit > this.commitIndex ? Math.min(args.commitIndex, this.log.length-1) : this.commitIndex

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
