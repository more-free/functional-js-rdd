var zerorpc = require("zerorpc");
var RddWorker = require('./RddWorker.js').RddWorker;
var rddWorker = new RddWorker('./somefile');

var server = new zerorpc.Server({
	// heartbeat
    hello: function(name, reply) {
        reply(null, "Hello, " + name);
    },

    // actions, support only linear stage now
    count : function(trans, reply) {
    	var cnt = rddWorker.count(trans);
    	reply(null, cnt);
    }
});

server.bind("tcp://0.0.0.0:4242");