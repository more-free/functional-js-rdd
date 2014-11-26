var util = require('util'),
	_ = require('underscore')._,
	futil = require('./functional_util.js'),
	fs = require('fs'),
	zerorpc = require("zerorpc"),
	partitioner = require("./Partitioner.js"),
	scheduler = new (require("./Scheduler.js").Scheduler)(),
	RPCClient = require("./client.js").RPCClient;


/** immutable data structure */
function RDD (workerNodes) {
	// parent data set, typically contains file path and partition
	// each element typically has a <ip>, a <port>, a <isInMem> field 
	this.dataPartition = workerNodes;

	// a set of dependencies (or parent RDDs) for each element of dataPartition, 
	// an example :
	// [
	//  { parent : parentRDD1, partition : [ index1, index2] }, 
	//  { parent : parentRDD2, partition : [ index1 ] }
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

// local file for test only
RDD.prototype.fromLocalFile = function(localFile) {
	var from = 1;
	var each = 10;

	this.dataPartition = this.dataPartition.map(function(p) {
		var np = { 
				   ip : p.ip, 
				   port : p.port, 
				   type : 'local', 
				   path : localFile, 
				   from : from, 
				   to : from + each 
				};
		from += each;
		return np;
	});

	return this;
}

RDD.prototype.fromHDFS = function(hdfsPath) {
	// TODO
}


/** 
*	one-to-one transformations, narrow dependency
*/
RDD.prototype.map = function(f) {
	return this.identity('map', f);
}

RDD.prototype.flatMap = function(f) {
	return this.identity('flatMap', f);
}

RDD.prototype.filter = function(f) {
	return this.identity('filter', f);
}

RDD.prototype.identity = function(typeName, f) {
	var parent = this;

	var dataPartition = this.dataPartition.map(function(p) {
		return { ip : p.ip, port : p.port, isInMem : false };
	});

	var dependency = _.range(this.dataPartition.length).map(function(d) {
		return { parent : parent, partition : [d] };
	});

	var transformation = { type : typeName, func : f };

	return this.build(dataPartition, dependency, transformation);
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
		this.getPartitioner(partitioner)
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
	var rdd = this;
	scheduler.runStagesSync(rdd, function(err, res) {
		if(err)
			throw new Error("stage error in <count>");

		scheduler.runCount(rdd, cb);
	});
}

RDD.prototype.collect = function(cb) {
	var rdd = this;
	scheduler.runStagesSync(rdd, function(err, res) {
		if(err)
			throw new Error('stage error in <collect>');

		scheduler.runCollect(rdd, cb);
	});
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
scheduler.setRemoteCmdStrategy(new RPCClient());

var rdd = new RDD([{ip : '127.0.0.1', port : 4242}])
.fromLocalFile("./somefile")
.flatMap(function(s){ 
      return s.trim().split(/\s+/); 
    })
.filter(function(s) {
	return s[0] === 'l';
})
.map(function(s) {
	return s.toUpperCase() + ' U !';
});

/*
.reduce(function(t1, t2) {
	return t1 + ":99" + t2;
});
*/

console.log(rdd.dataPartition);
console.log(rdd.dependency);
console.log(rdd.transformation);

var stages = scheduler.buildStages(rdd);
console.log(stages);

rdd.count(function(err, cnt) {console.log(cnt); });
rdd.collect(function(err, res) { console.log(res); });

