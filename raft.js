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
 */

var q = require('q')

module.exports = function (socket, ip, serverList, sm) {
  // Persistent state on all servers
  this.role = 'follower' // [follower, candidate, leader]
  this.currentTerm = 0
  this.votes = 0
  this.votedFor = null
  this.log = [] // Three states: served -> executed
  this.socket = socket
  this.serverList = serverList
  this.serverId = null // Current server id
  this.leaderId = null
  this.voteFor = {}
  this.electionTimeout = Math.floor(Math.random() * 300 + 150) // 150-300 ms
  this.ip = ip
  this.id = serverList.find(function (server) {
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
  this.serverList.forEach(function (server) {
    self.nextIndex[server.id] = self.log.length
    self.matchIndex[server.id] = 0
  })

  this.multicast = function (cb) {
    return q.all(this.serverList.filter(function (server) {
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
      var ackId = parseInt(message.dest_id) + parseInt(message.messageIndex)
      console.log('storing ack id at ' + ackId)
      this.waitingAcks[ackId] = dfd
      setInterval(function () {
        // Resend indefinitely if not receive ack in timeout
        self.socket.send(JSON.stringify(message))
      }, this.ackTimeout)
      return dfd.promise
    }
  }

  this.isUpToDate = function (term) {
    if (term > this.currentTerm)
      this.role = 'follower'
    return term >= this.currentTerm
  }

  this.eventLoop = function () {
    var self = this
    setInterval(function () {
      var role = self.role
      if (role == 'follower' || role == 'candidate') {
        self.timeSinceLastHeartbeatFromLeader += 1

        // So weird, assigning to a variable works
        var time = self.timeSinceLastHeartbeatFromLeader
        var timeout = self.electionTimeout
        if (time > timeout) {
          console.log('start election')
          // Restart election
          self.electionTimeout = Math.floor(Math.random() * 300 + 150) // 150-300 ms
          self.currentTerm += 1
          self.role = 'candidate'
          self.votes = 1 // Vote for self
          self.voteFor[self.currentTerm] = self.id
          self.leaderId = null
          self.timeSinceLastHeartbeatFromLeader = 0
          self.multicast(function (server) {
            return self.requestVote(server.id).then(function (results) {
              console.log('[REQUESTVOTE] ' + server.id + (results.voteGranted ? '' : ' NOT ') +  ' voted me (' + self.id + ')')
              if (results.voteGranted)
                self.votes += 1

              // Majority
              var votes = self.votes
              var half = self.serverList.length / 2
              if (votes > half) {
                console.log('I won')
                self.role = 'leader'
                self.leaderId = self.id

                self.serverList.forEach(function (server) {
                  self.nextIndex[server.id] = self.log.length
                  self.matchIndex[server.id] = 0
                })
              }
            })
          })
        }
      }

      function updateEntries(serverId) {
        var index = self.nextIndex[serverId],
            entries = self.log.slice(index, index+1)
        self.appendEntries(serverId, entries).then(function (results) {
          if (!results.success) {
            // Update term??
            self.nextIndex[serverId] -= 1
            updateEntries()
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
        // Heartbeat
        self.multicast(function (server) {
          return self.appendEntries(server.id)
        })
      }
    }, self.electionTimeout)
  }

  this.execute = function (index) {
    var command = this.log[index][0],
        data    = this.log[index][1]
    this.sm[command].apply(this, data)
    this.log[index].state = 'executed'
    this.commitIndex = this.commitIndex < index ? index : this.commitIndex
    this.lastApplied = index
  }

  this.leader = function () {
    this.serverList.find(function (server) {
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
        var ackId = parseInt(data.dest_id) + parseInt(data.messageIndex)
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

    self.log.push({command: [command, data], term: this.currentTerm, state: 'served'})

    return self.multicast(function (server) {
      var index = this.nextIndex[server.id],
          entries = this.log.slice(index, index+1)

      return self.appendEntries(server.id, entries).then(function (results) {
        if (!results.success) {
          // Update term??
        } else {
          self.matchIndex[server.id] = index
          self.nextIndex[server.id] += 1
        }
      })
    }).then(function () {
      // Replicated to all
      self.execute()
    })
  }

  // Only invoked by leader
  this.appendEntries = function (destId, entries) {
    return this.rpc(destId, 'appendEntries', {
      term: this.currentTerm,
      leaderId: this.leaderId,
      prevLogIndex: this.log.length-2 >= 0 ? this.log.length-2 : null,
      prevLogTerm: this.log.length-2 >= 0 ? this.log[this.log.length-2].term : null,
      entries: entries || [], // For now entries contain only one entry
      leaderCommit: this.commitIndex,
    })
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

        if (role == 'candidate') {
          this.role = 'follower'
        } 

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
        if (!this.isUpToDate(args.term)) {
          console.log('reject because it is not up to date')
          return {func: args.func, term: this.currentTerm, voteGranted: false}
        }

        if (!this.voteFor[args.term]) {
          if (this.log[args.lastLogIndex]) {
            if (this.log[args.lastLogIndex].term <= args.lastLogTerm) {
              console.log('accepted because the term of lastLog is equal or higher than me')
              this.voteFor[args.term] = args.candidateId
              return {func: args.func, term: this.currentTerm, voteGranted: true}
            } else {
              console.log('reject because the term of lastLog is less than me')
              return {func: args.func, term: this.currentTerm, voteGranted: false}
            }
          } else {
            console.log('accepted because the term of lastLog is equal or higher than me')
            this.voteFor[args.term] = args.candidateId
            return {func: args.func, term: this.currentTerm, voteGranted: true}
          }
        } else {
          console.log('reject because the I have already voted')
          return {func: args.func, term: this.currentTerm, voteGranted: false}
        }
        break
    }
  }
}
