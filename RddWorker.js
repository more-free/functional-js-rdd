var fs = require('fs');
var futil = require('./functional_util.js');

/**
*  a worker holds on an immutable part of data, unless persist() is called.
*/
function RddWorker(partition) {
	this.partition = partition;
	this.data = [];
}

RddWorker.prototype.loadDataIfNecessary = function() {
	// not in memory yet
	if(this.data.length === 0) { 
		// now assume it is in a local file system (not support HDFS yet)
		var fstr = fs.readFileSync(this.partition).toString();
		this.data.push(fstr);
	}
}

RddWorker.prototype.applyLinearTrans = function(trans) {
	this.loadDataIfNecessary();
	
	var obj = this;
	var data = this.data;
	trans.forEach(function(f){
		data = obj.applyTrans(data, f);
	})

	return data;
}

RddWorker.prototype.applyTrans = function(data, f) {
	var func = eval('(' + f.func + ')');

	switch(f.type) {
		case 'map' :
			return this.map(data, func);
		case 'filter' :
			return this.filter(data, func);
		case 'flatMap' :
			return this.flatMap(data, func);
		case 'reduce' :
			return this.reduce(data, func, f.initialValue);
		default :
			return data;
	}
}

/** transformations */
RddWorker.prototype.map = function(data, f) {
	return data.map(f);
}

RddWorker.prototype.filter = function(data, f) {
	return data.filter(f);
}

RddWorker.prototype.flatMap = function(data, f) {
	return futil.flatMap(data, f);
}

RddWorker.prototype.reduce = function(data, f, initialValue) {
	return futil.existy(initialValue) ? 
		data.reduce(f, initialValue) : data.reduce(f);
}

/** actions */
RddWorker.prototype.count = function(trans) {
	var data = this.applyLinearTrans(trans);
	return data.length;
}

exports.RddWorker = RddWorker;
