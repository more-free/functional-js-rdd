var util = require('util'),
	_ = require('underscore')._,
	futil = require('./functional_util.js'),
	fs = require('fs'),
	zerorpc = require("zerorpc"),
	partitioner = require("./Partitioner.js");


/** immutable data structure */
function RDD () {
	// parent data set, typically contains file path and partition
	// each element must have a <ip> and a <port> field 
	this.dataPartition = [];

	// a set of dependencies (or parent RDDs) for each element of dataPartition, 
	// an example :
	// [
	//  { parent : parentRDD1, partition : [ p1, p2] }, 
	//  { parent : parentRDD2, partition : [ p1 ] }
	// ]
	this.dependency = [];

	// a single function f, applying f on parent RDDs produces target RDD
	this.transformation = {};

	// works for (key, value) pair-RDD only. by default it uses the partitioner
	// of its parent RDD (Note although it is a wide-dependency, it still has only
	// one parent RDD). If its parent RDD has no partitioner set, it uses hash
	// partitioner by default
	this.partitioner = null;
}

RDD.prototype.build = function(dataPartition, dependency, transformation, partitioner) {
	var target = new RDD();
	target.dataPartition = dataPartition;
	target.dependency = dependency;
	target.transformation = transformation;
	target.partitioner = partitioner;

	return target;
}

RDD.prototype.fromLocalText = function(localFile) {

}

/** note this operation will change the state of RDD */
RDD.prototype.addDataPartition = function(partition) {
	this.dataPartition.push(partition);
	return this;
}

/** 
*	one-to-one transformations, narrow dependency
*/
RDD.prototype.map = function(f) {
	var parent = this;

	return this.build(
		this.dataPartition, 
		this.dataPartition.map(function(p) {
			return { parent : parent, partition : [p] }
		}),
		{ type : 'map', func : f }
		);
}

RDD.prototype.flatMap = function(f) {
	var parent = this;

	return this.build(
		this.dataPartition,
		this.dataPartition.map(function(p) {
			return { parent : parent, partition : [p] }
		}),
		{ type : 'flatMap', func : f }
		);
}

RDD.prototype.filter = function(f) {
	var parent = this;

	return this.build(
		this.dataPartition,
		this.dataPartition.map(function(p) {
			return { parent : parent, partition : [p] }
		}),
		{ type : 'filter', func : f }
		);
}

/**
*	one-to-one transformations, wide dependencies, 
*   some of them are for key-value RDD only
*/

// RDD[(K, V)] => RDD[(K, Array[V])]
RDD.prototype.groupByKey = function(partitioner) {
	var parent = this;

	return this.build(
		this.dataPartition,
		this.dataPartition.map(function(p) {
			return { parent : parent, partition : parent.dataPartition }
		}),
		{ type : 'groupByKey' },
		this.getPartitioner(partitioner);
		);
}

// RDD[(K, V)] => RDD[(K, V)]
RDD.prototype.reduceByKey = function(f, partitioner) {

}

RDD.prototype.distinct = function(partitioner) {

}

RDD.prototype.sort = function(f, partitioner) {

}


RDD.prototype.getPartitioner = function(userSpecifiedPartitioner) {
	if(futil.existy(userSpecifiedPartitioner))
		return userSpecifiedPartitioner;
	else if(futil.existy(this.partitioner)) 
		return this.partitioner;
	else
		return partitioner.simpleHashPartition;
}


/**
*	multiple-to-one transformations
*/
RDD.prototype.union = function(otherRdd) {
	return this.build(
		this.dataPartition.concat(otherRdd.dataPartition),
		this.dependency.concat(otherRdd.dependency),
		{ type : 'union' }
		);
}

RDD.prototype.intersect = function(otherRdd) {

}

RDD.prototype.subtract = function(otherRdd) {

}

// for key-value pair RDD only
RDD.prototype.crossProduct = function(otherRdd, partitioner) {

}


/** 
* actions. 
* TODO : decoupled with the underlying zeroRPC; move to scheduler; add failover
*/

// TODO  modify reduce as an action instead of a transformation
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

RDD.prototype.collect = function(cb) {

}

RDD.prototype.persist = function(cb) {

}

RDD.prototype.save = function(saveObj, cb) {

}

/** 
* private functions 
*/

// get all one-to-one narrow transformations so far 
RDD.prototype.getLinearTrans = function() {
	var trans = [];
	var last = this;

	while(last.dependency.length !== 0) {
		trans.push(last.transformation);
		last = last.dependency[0].parent;
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
