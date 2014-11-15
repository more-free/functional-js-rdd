var util = require('util'),
	_ = require('underscore')._,
	futil = require('./functional_util.js'),
	fs = require('fs'),
	zerorpc = require("zerorpc");


/** immutable data structure */
function RDD () {
	// parent data set, typically contains file path and partition
	// each element must have a <ip> and a <port> field 
	this.dataPartition = [];

	// a set of dependencies (or parent RDDs)
	this.dependency = [];

	// a function f, applying f on parent RDDs produces target RDD
	this.transformation = {};
}

RDD.prototype.build = function(dataPartition, dependency, transformation) {
	var target = new RDD();
	target.dataPartition = dataPartition;
	target.dependency = dependency;
	target.transformation = transformation;

	return target;
}

RDD.prototype.fromLocalText = function(localFile) {

}

/** note this operation will change the state of RDD */
RDD.prototype.addDataPartition = function(partition) {
	this.dataPartition.push(partition);
	return this;
}

RDD.prototype.map = function(f) {
	return this.build(this.dataPartition, 
		[this],
		{ type : 'map', func : f }
		);
}

RDD.prototype.flatMap = function(f) {
	return this.build(this.dataPartition,
		[this],
		{ type : 'flatMap', func : f }
		);
}

RDD.prototype.filter = function(f) {
	return this.build(this.dataPartition,
		[this],
		{ type : 'filter', func : f }
		);
}

RDD.prototype.reduce = function(f, initialValue) {
	return this.build(this.dataPartition,
		[this],
		{ type : 'reduce', func : f, initialValue : initialValue }
		);
}

RDD.prototype.count = function(cb) {
	var trans = this.serializeTrans(this.getLinearTrans());
	var cnt = 0;
	var clients = [];
	var todo = this.dataPartition.length;
	var done = 0;

	this.dataPartition.forEach(function(p) {
		var client = new zerorpc.Client();
		clients.push(client);
		client.connect("tcp://" + p.ip + ":" + p.port);

		client.invoke("count", trans, function(err, res, more) {
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

/** get all transformations so far */
RDD.prototype.getLinearTrans = function() {
	var trans = [];
	var last = this;

	while(last.dependency.length !== 0) {
		trans.push(last.transformation);
		last = last.dependency[0];
	}

	return trans.reverse();
}

RDD.prototype.serializeTrans = function(trans) {
	return trans.map(function(t) {
		var s = _.clone(t);
		s.func = s.func.toString();
		return s;
	});
}

// test. eventually these lines will be moved to RddDriver.js
var rdd = new RDD();

rdd
.addDataPartition({ip : '127.0.0.1', port : 4242})
.flatMap(function(s){ 
      return s.trim().split(/\s+/); 
    })
.filter(function(s) {
	return s[0] === 't' || s[0] === 'T';
})
.reduce(function(t1, t2) {
	return t1 + ":99" + t2;
})
.count(function(err, cnt) {
	console.log(cnt);
});
