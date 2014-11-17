/**
*  build stages for an action of a given RDD, run each stage until success. 
*  a typical stage has a boundary of a shuffle operation between data partitions.
*/

var RDD = require("./RDD.js").RDD;
var futil = require("./functional_util.js");

function Scheduler {

} 

/** TODO naive DAG implementation, not efficient */
Scheduler.prototype.mkDAG = function(RDD, action) {
	var dag = new DAG();
	var queue = [RDD];

	while(queue.length > 0) {
		var cur = queue.shift();
		cur.dependency.forEach(function(den) {
			dag.addNode(den.parent);
			dag.addEdge(den.parent, cur, cur.transformation.type);

			if(queue.indexOf(den.parent) < 0)
				queue.push(den.parent);
		});
	}

	return dag;
}

Scheduler.prototype.buildStages = function(dag) {
	
}

function DAG() {
	// reference to RDDs
	this.nodes = [];  
	// { from : <idx in nodes>, to : <idx in nodes>, edge : <name of transformations> }
	this.edges = [];  
}

DAG.prototype.addNode = function(n) {
	var idx = this.nodes.indexOf(n);
	if(idx >= 0) 
		return idx;
	else {
		this.nodes.push(n);
		return this.nodes.length - 1;
	}
}

DAG.prototype.addEdge = function(from, to, edge) {
	var fromIdx = this.addNode(from);
	var toIdx = this.addNode(to);

	var idx = this.findEdge(fromIdx, toIdx, edge);
	if(idx >= 0) 
		return idx;
	else {
		this.edges.push({ from : fromIdx, to : toIdx, edge : edge });
		return edges.length - 1;
	}
}

DAG.prototype.findEdge = function(fromIdx, toIdx, edge) {
	for(var i = 0; i < this.edges.length; i++) {
		var cur = this.edges[i];
		if(cur.from === fromIdx && cur.to === toIdx && cur.edge == edge) 
			return i;
	}
	return -1;
}