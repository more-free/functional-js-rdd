var zerorpc = require("zerorpc");
var util = require("util");
var _ = require("underscore")._;


function RemoteCmdStrategy() {

}

RemoteCmdStrategy.prototype.setUrl = function(url) {
	throw new Error("not implemented");
}

RemoteCmdStrategy.prototype.linearTransform = function() {
	throw new Error("not implemented");
}

RemoteCmdStrategy.prototype.count = function() {
	throw new Error("not implemented");
}

RemoteCmdStrategy.prototype.collect = function() {
	throw new Error("not implemented");
}


/**
* @param url ex. tcp://127.0.0.1:4242
*/
function RPCClient() {
	this.url = '';
}
util.inherits(RPCClient, RemoteCmdStrategy);

RPCClient.prototype.setUrl = function(url) {
	this.url = url;
	return this;
}

RPCClient.prototype.linearTransform = function(trans, cb) {
	var client = new zerorpc.Client();
	var url = "tcp://" + trans.source.ip + ":" + trans.source.port;
 
	client.connect(url); 
	client.invoke("linearTransform", trans, function(err, keyInMem, more) { 
		cb(err, keyInMem); // err is null if succeeded
		client.close(); // it's ok for async close event
	})
}

RPCClient.prototype.count = function(RDD, cb) {
	var cnt = 0;
	var clients = [];
	var todo = RDD.dataPartition.length;
	var done = 0;

	RDD.dataPartition.forEach(function(p) {
		var client = new zerorpc.Client();
		clients.push(client);
		client.connect("tcp://" + p.ip + ":" + p.port);

		client.invoke("count", p, function(err, res, more) {
			cnt += parseInt(res);
			done += 1;
			if(done === todo) { 
				try {
					clients.forEach(function(c) {
						c.close();
					});
					cb(null, cnt);
				} catch(err) {
					cb(err, cnt);
				}
			}
		});
	});
}

RPCClient.prototype.collect = function(RDD, cb) {
	var coll = [];
	var clients = [];
	var todo = RDD.dataPartition.length;
	var done = 0;

	RDD.dataPartition.forEach(function(p) {
		var client = new zerorpc.Client();
		clients.push(client);
		client.connect("tcp://" + p.ip + ":" + p.port);

		client.invoke("collect", p, function(err, res, more) {
			coll.push(res);
			done += 1;
			if(done === todo) { 
				var flattenColl = _.flatten(coll);

				try {
					clients.forEach(function(c) {
						c.close();
					});
					cb(null, flattenColl);
				} catch(err) {
					cb(err, flattenColl);
				}
			}
		});
	});
}

exports.RemoteCmdStrategy = RemoteCmdStrategy;
exports.RPCClient = RPCClient;
