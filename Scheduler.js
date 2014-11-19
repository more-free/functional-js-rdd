/**
*  Build stages for an action of a given RDD, run each stage until success. 
*  a typical stage has a boundary of a shuffle operation between data partitions.
*  Stages are represented by implicit DAG. 
*/

var RDD = require("./RDD.js").RDD;
var futil = require("./functional_util.js");
var _ = require("underscore")._;

function Scheduler {

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
Scheduler.prototype.runStage = function(RDD) {
	if(this.isShuffle(RDD)) {
		this.runShuffle(RDD);
	} else {
		// run trasformations for each partition of the target RDD
		var trans = this.getTransformation(RDD);
		for(var i = 0; i < trans.length; i++) 
			this.runTransformations(trans[i], RDD.dataPartition[i]);
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
	
}

// { 
//	source : some source RDD dataPartition element, 
//	trans : [a list of transformations, ex. {type : 'map', func : xxx}, {type : 'filter', func : xxx}]
//  }
Scheduler.prototype.getTransformation = function(RDD) {
	// TODO
}

Scheduler.prototype.runTransformations = function(trans, dependency) {
	// TODO
}

Scheduler.prototype.runAction = function(RDD, action) {
	// TODO
}
