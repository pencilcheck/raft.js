/*
 * TODO:
 * 5.1 Basics [DONE]
 * 5.2 Leader election [DONE]
 * 5.3 Log replication
 * 5.4 Safety
 * 5.4.1, 5.4.2, 5.4.3
 * 5.5 Follower and candidate crashes
 * 5.6 Timing and availability
 * 6   Cluster membership changes
 * 7.  Log compaction
 * 8.  Client interaction
 */

module.exports = function (socket, ip, serverList) {
  // Persistent state on all servers
  this.role = 'follower' // [follower, candidate, leader]
  this.currentTerm = 0
  this.votes = 0
  this.votedFor = null
  this.log = []
  this.socket = socket
  this.serverList = serverList
  this.serverId = null // Current server id
  this.leaderId = null
  this.electionTimeout = Math.floor(Math.random() * 300 + 150) // 150-300 ms
  this.ip = ip
  this.id = serverList.find(function (server) {
    return server.ip == ip
  }).id

  // Volatile state on all servers
  this.commitIndex = 0
  this.lastApplied = 0
  this.timeSinceLastHeartbeatFromLeader = 0 // in ms

  // Volatile state on leaders
  this.nextindex = []
  this.matchIndex = []

  this.constructVoteForMessage = function (votedTerm) {
    return {
      type: 'voteFor',
      term: votedTerm
    }
  }

  this.broadcast = function (message) {
    this.serverList.forEach(function (server) {
      if (server.id != this.id)
        this.send(server.id, message)
    }, this)
  }

  this.isUpToDate = function (term) {
    return term >= this.currentTerm
  }

  this.printMessage = function (who, message) {
    console.log(who, message)
  }

  this._eventLoop = function () {
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
          self.votes = 0
          self.leaderId = null
          self.timeSinceLastHeartbeatFromLeader = 0
          self.broadcast(self._requestVote())
        }
      }

      if (self.role == 'leader') {
        // heartbeat to followers
        self.broadcast(self._appendEntries())
      }
    })
  }

  this.send = function (id, message) {
    console.log('sending', message.type, 'to', id, 'from', this.id)
    this.socket.send(JSON.stringify({src_id: this.id, dest_id: id, message: message}))
  }

  this.set = function (command, data) {
  }

  this.leader = function () {
    this.serverList.find(function (server) {
      return server.id == this.leaderId
    })
  }

  this.start = function () {
    var self = this
    this.socket.onmessage = function (event) {
      console.log('onmessage', event.data)
      var data = JSON.parse(event.data)
      if (data.who == 'onopen') {
        self.serverId = data.message
        self.send('setup', [self.serverId, self.id])
        self._eventLoop()
      } else
        self._respondToRequest(data.who, data.message)
    }

    this.socket.onerror = function (event) {
      console.log('onerror', event)
    }

    this.socket.onclose = function (event) {
      console.log('onclose', event)
    }
  }

  // Only invoked by leader
  this._appendEntries = function () {
    if (this.role != 'leader')
      return

    return {
      type: 'appendEntries',
      term: this.currentTerm,
      leaderId: this.leaderId,
      prevLogIndex: null,
      prevLogTerm: null,
      entries: [],
      leaderCommit: null,
    }
  }

  // Only invoked by candidate
  this._requestVote = function () {
    if (this.role != 'candidate')
      return

    return {
      type: 'requestVote',
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex: this.log.length-1,
      lastLogTerm: this.log[-1] && this.log[-1].term || 0
    }
  }

  this._respondToRequest = function (who, message) {
    //this.printMessage(who, message)
    if (this.isUpToDate(message.term)) {
      console.log('up to date')
      switch(message.type) {
        case 'appendEntries':
          this.leaderId = who
          break
      }

      var role = this.role
      if (role == 'candidate') {
        switch(message.type) {
          case 'appendEntries':
            this.role = 'follower'
            this.electionTimeout = Math.floor(Math.random() * 300 + 150) // 150-300 ms
            this.timeSinceLastHeartbeatFromLeader = 0
            break
          case 'voteFor':
            this.votes += 1
            // Majority
            console.log(this.votes, this.serverList.length / 2)
            var votes = this.votes
            var half = this.serverList.length / 2
            if (votes > half) {
              console.log('I won')
              this.role = 'leader'
              this.leaderId = this.id
            }
            break
        }
      }

      if (message.type == 'requestVote') {
        if (!this.voteFor) {
          this.voteFor = message.candidateId
          this.send(this.voteFor, this.constructVoteForMessage(message.term))
        }
      }
    }
  }
}
