var express = require('express'),
    bodyParser = require('body-parser'),
    compression = require('compression'),
    data = require('./result'),
    app = express();

app.set('port', process.env.PORT || 5000);
global.count = 0;

var mysql = require('mysql');
var connection = mysql.createConnection({host: 'localhost', user: 'root', password: '', database: 'bigdata'});
connection.connect();

app.use(bodyParser.json());
app.use(compression());

app.use('/', express.static(__dirname + '/www'));
app.get('/dataclean', data.dataClean.bind(this, connection));

app.use(function(err, req, res, next) {
    console.error(err.stack);
    res.status(500).send(err);
});

app.listen(app.get('port'), function () {
    console.log('Express server listening on port ' + app.get('port'));
});

process.on('SIGINT', function() {
    // todo sth
    connection.end();
    global.count = 0;
    console.log("stop server");
    process.exit(0);
});
