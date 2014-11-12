var http = require('http');

var server = http.createServer( function(req, res) {
	var str = '';

	req.setEncoding('utf-8');
	req.on('data', function(p) {
		str += p;
	});
	req.on('end', function(p) {
		str += '\n';
		res.end(str);
	});
} );

server.listen(3000);