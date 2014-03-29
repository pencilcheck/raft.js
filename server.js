require('es6-shim')

var RaftServer      = require('./raft.js'),
    WebSocket       = require('faye-websocket'),
    http            = require('http'),
    express         = require('express'),
    app             = express(),
    port            = process.env.PORT || (process.argv.indexOf('-p') > -1 && process.argv[process.argv.indexOf('-p')+1]) || 5000,
    serverList      = [
      {id: 1, ip: 'localhost:5000'},
      {id: 2, ip: 'localhost:5001'},
      {id: 3, ip: 'localhost:5002'},
      {id: 4, ip: 'localhost:5003'},
      {id: 5, ip: 'localhost:5004'}],
    STM             = require('./sm.js'),
    sm              = new STM(),
    socketUrl       = 'ws://localhost:3000/',
    ws              = new WebSocket.Client(socketUrl),
    raftServer      = new RaftServer(ws, 'localhost:' + port, serverList, sm)

app.use(express.static(__dirname + '/'))

app.get('/', function (req, res) {
  if (raftServer.role == 'leader') {
    raftServer.serve(req.param('command'), JSON.parse(req.param('data'))).then(function (result) {
      res.send(result)
    })
  } else {
    // Redirect to leader
    // http://stackoverflow.com/questions/17612695/expressjs-how-to-redirect-a-post-request-with-parameters
    console.log('redirect client request to leader', raftServer.leader().id)
    res.redirect(307, raftServer.leader().ip + req.path)
  }
})

var server = http.createServer(app)
server.listen(port)

console.log('http server listening on %d', port)

raftServer.start()
