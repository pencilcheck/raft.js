require('es5-shim')
require('es6-shim')

var RaftServer      = require('./raft.js'),
    WebSocket       = require('faye-websocket'),
    http            = require('http'),
    express         = require('express'),
    app             = express(),
    port            = process.env.PORT || process.argv[process.argv.indexOf('-p')+1] || 5000,
    serverList      = [
      'http://localhost:5000',
      'http://localhost:5001',
      'http://localhost:5002',
      'http://localhost:5003',
      'http://localhost:5004'],
    STM             = require('./sm.js'),
    sm              = new STM(),
    socketUrl       = 'ws://localhost:3000/',
    ws              = new WebSocket.Client(socketUrl),
    raftServer      = new RaftServer(ws, 'http://localhost:' + port, serverList, sm),
    debug           = process.argv.indexOf('--debug') > -1 && process.argv[process.argv.indexOf('--debug')+1] || false

app.use(express.static(__dirname + '/'))

function redirectToLeader(res, url) {
  // Redirect to leader
  // http://stackoverflow.com/questions/17612695/expressjs-how-to-redirect-a-post-request-with-parameters
  console.log('redirect client request to leader at', url)
  res.redirect(307, url)
}

app.use(express.bodyParser());

app.post('/', function (req, res) {
  if (raftServer.role == 'leader') {
    raftServer.serve(req.param('command'), req.param('data'))
      .then(function (results) {
        res.send(JSON.stringify(results))
      })
  } else {
    redirectToLeader(res, raftServer.leader() + req.path)
  }
})

app.post('/configure', function (req, res) {
  if (raftServer.role == 'leader') {
    raftServer.serve('configuration', ['setServers'].concat(req.param('data')))
      .then(function (result) {
        res.send(JSON.stringify(result))
      })
  } else {
    redirectToLeader(res, raftServer.leader() + req.path)
  }
})

app.post('/configure/add', function (req, res) {
  if (raftServer.role == 'leader') {
    raftServer.serve('configuration', ['addServers'].concat(req.param('data')))
      .then(function (result) {
        res.send(JSON.stringify(result))
      })
  } else {
    redirectToLeader(res, raftServer.leader() + req.path)
  }
})

app.delete('/configure', function (req, res) {
  if (raftServer.role == 'leader') {
    raftServer.serve('configuration', ['removeServers'].concat(req.param('data')))
      .then(function (result) {
        res.send(JSON.stringify(result))
      })
  } else {
    redirectToLeader(res, raftServer.leader() + req.path)
  }
})

var server = http.createServer(app)
server.listen(port)

console.log('http server listening on %d', port)


if (!debug) {
  console.log = function () {
  }
} else {
  console.log('debug mode')
}

raftServer.start()
