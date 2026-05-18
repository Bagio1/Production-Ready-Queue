const http = require('http');
const config = require('./config');

function createStatusServer(port, component) {
  const server = http.createServer((req, res) => {
    if (req.url === config.server.healthPath) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', component }));
      return;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ component, port, timestamp: new Date().toISOString() }));
  });

  server.listen(port, () => {
    console.log(`${component} HTTP status server listening on port ${port}`);
  });

  return server;
}

module.exports = {
  createStatusServer
};
