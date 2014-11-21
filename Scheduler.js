/**
*  Build stages for an action of a given RDD, run each stage until success. 
*  a typical stage has a boundary of a shuffle operation between data partitions.
*  Stages are represented by implicit DAG. 
*/

var RDD = require("./RDD.js").RDD;
var futil = require("./functional_util.js");
var _ = require("underscore")._;
var async = require("async");

/**
* @param remoteCmdStrategy it might be RPC, MQ, Http, etc.
*/
function Scheduler(remoteCmdStrategy) {
	this.remoteCmdStrategy = remoteCmdStrategy;
} 

Scheduler.prototype.setRemoteCmdStrategy = function(remoteCmdStrategy) {
	this.remoteCmdStrategy = remoteCmdStrategy;
}


Scheduler.prototype.runStagesSync = function(RDD, callback) {
	var targetRDDS = this.buildStages(RDD);
	var obj = this;
	var seriesFuncs = targetRDDS.map(function(rdd) {
		return function(cb) {
			obj.runStage(rdd, cb); 
		}
	});

	async.series(seriesFuncs, callback);
}


/**
* @param RDD final RDD (before applying the action)
* @return an array of target RDDs for each stage. Each target RDD represents a DAG implicitly.
*/
Scheduler.prototype.buildStages = function(RDD) {
	var parentRDDs = RDD.dependency.map(function(den) { return den.parent; } );

	if(parentRDDs.length === 0) {
		return [RDD];
	} else if(this.isShuffle(RDD)) {  // TODO multiple shuffle
		return this.buildStages(parentRDDs[0]).concat([RDD]);
	} else if(parentRDDs.length > 1) { // multiple transformation
		if(RDD.transformation.type === 'union') {
			var leftParentStages = this.buildStages(parentRDDs[0]);
			var rightParentStages = this.buildStages(parentRDDs[1]);

			return futil.exceptLast(leftParentStages)
					.concat(futil.exceptLast(rightParentStages))
					.concat([RDD]);
		} 
		// TODO else handle other multiple transformations
	} else {  // one-to-one transformation
		var parentStages = this.buildStages(parentRDDs[0]);
		return futil.setLast(parentStages, RDD);
	}
}

// TODO support more shuffle operations
Scheduler.prototype.isShuffle = function(RDD) {
	return ['groupByKey'].indexOf(RDD.transformation.type) >= 0;
}

/**
* @param RDD target RDD. it might be the target RDD for any stage 
*/
Scheduler.prototype.runStage = function(RDD, cb) {
	if(this.isShuffle(RDD)) {
		this.runShuffle(RDD, cb);
	} else {
		// run trasformations for each partition of the target RDD
		var trans = this.getLinearTransformationForEachPartition(RDD);
		var obj = this;
		var todo = trans.length;
		var done = 0;

		_.range(trans.length).forEach(function(i) {
			obj.runLinearTransformations(trans[i], function(err, res) {
				if(err) 
					throw new Error('stage error');
				done ++; 
				if(done === todo) 
					cb(null, null);
			});
		});
	}
}

Scheduler.prototype.runShuffle = function(RDD) {
	// TODO
	switch(RDD.transformation.type) {
		case 'groupByKey' :
			this.runGroupByKey(RDD);
			break; 
	}
}

Scheduler.prototype.runGroupByKey = function(RDD) {
	// TODO										
}

 
//	trans : [a list of transformations, ex. {type : 'map', func : xxx}, {type : 'filter', func : xxx}]
Scheduler.prototype.getLinearTransformationForEachPartition = function(RDD) {
	var obj = this;
	return RDD.dependency.map(function(d) { 
		return obj.findSource(d, RDD); 
	});
}

Scheduler.prototype.findSource = function(dependency, RDD) {
	var obj = this;
	var trans = [];
	var source = [];

	while(true) {
		trans.push(RDD.transformation);

		RDD = dependency.parent;
		var parentPartitionIndex = dependency.partition[0];
		var parentPartition = RDD.dataPartition[parentPartitionIndex];

		if(parentPartition.isInMem || RDD.dependency.length === 0) {
			source = parentPartition;
			break;
		}

		dependency = RDD.dependency[parentPartitionIndex];
	}

	return  {
			source : source, 
			trans : trans.reverse().map(function(tran) 
						{ return obj.serialTransformation(tran); }
					)
			};
}

Scheduler.prototype.serialTransformation = function(tran) {
	return { type : tran.type, func : tran.func.toString() };
}


// usually targetDataPartition is the same as trans.source
Scheduler.prototype.runLinearTransformations = function(trans, cb) {
	this.remoteCmdStrategy.linearTransform(trans, cb);
}

Scheduler.prototype.runCount = function(RDD, cb) {
	this.remoteCmdStrategy.count(RDD, cb);
}

Scheduler.prototype.runCollect = function(RDD, cb) {
	this.remoteCmdStrategy.collect(RDD, cb);
}


exports.Scheduler = Scheduler;
