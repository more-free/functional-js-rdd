var zerorpc = require("zerorpc");
var RddWorker = require('./RddWorker.js').RddWorker;
var rddWorker = new RddWorker('./somefile');

var server = new zerorpc.Server({
	// heartbeat
    hello: function(name, reply) {
        reply(null, "Hello, " + name);
    },

    // transformations
    linearTransform : function(trans, reply) {
    	rddWorker.linearTransform(trans, function(success) {
    		reply(null, success);
    	});
    },

    // actions, support only linear stage now
    count : function(partition, reply) {
    	reply(null, rddWorker.count(partition));
    },

    collect : function(partition, reply) {
    	reply(null, rddWorker.collect(partition));
    }
});

server.bind("tcp://0.0.0.0:4242");