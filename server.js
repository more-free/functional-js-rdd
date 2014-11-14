var http = require('http');
var RDD = require('./RDD.js').RDD;
var rdd = new RDD();


function Controller() {

}

Controller.prototype.handle = function(reqStr, res) {
	console.log("request string = " + reqStr);
	var req = JSON.parse(reqStr);
	switch(req.type) {
		case 'load' :
			rdd.load(req.url);
			res.end('load succeeded !');
			break;
		case 'map' :
			rdd.map(req.func);
			res.end('map succeeded !');
			break;
		case 'flatMap' :
			rdd.flatMap(req.func);
			res.end('flatMap succeeded !');
			break;
		case 'count' :
			var cnt = rdd.count(function(rdd){
				res.end('cnt = ' + rdd.data.length);
			});
	}
}


var controller = new Controller();
var server = http.createServer(function(req, res) {
	var str = '';

	req.setEncoding('utf-8');
	req.on('data', function(p) {
		str += p;
	});
	req.on('end', function(){
		controller.handle(str, res);
	});
} );

server.listen(3000);