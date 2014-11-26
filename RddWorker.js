var fs = require('fs');
var futil = require('./functional_util.js');
var lazy = require('lazy');

/**
*  a worker holds an immutable part of data, unless persist() is called.
*/
function RddWorker() {
	this.data = [];
}

/**
* load data into memory (if necessary); apply a series of linear transformations 
* to data, ex. map, filter, flatMap, etc. 
* @param trans { source : xxx, trans : [{xxx}, {xxx}] }
* @param callback(err, keyInMem)
*/
RddWorker.prototype.linearTransform = function(trans, callback) {
	var obj = this;
	var cb = function(err, keyInMem) {
		try {
			var res = obj.applyLinearTrans(trans.trans);
			obj.data = res.data;
			callback(null, res.key);
		} catch(err) {
			callback(err, null);
		}
	};

	if(!trans.source.isInMem) { // TODO do not use this ! this is from client 
		switch(trans.source.type) {
			case 'hdfs':
				this.loadHDFS(trans.source, cb);
				break;
			default :
				this.loadLocalFile(trans.source, cb);
		}
	} else {
		this.data = this.retrieveObject(trans.source.key);
		cb(true);
	}
}

RddWorker.prototype.retrieveObject = function(key) {
	// TODO
}

RddWorker.prototype.loadLocalFile = function(dataPartition, cb) {
	var lineCnt = 0;
	var obj = this;

	// TODO not efficient, because it will read all lines. need to find way to end early
	// TODO do not use lazy, it is not active.
	new lazy(fs.createReadStream(dataPartition.path))
			.on('end', function() { 
 						cb(true);
 					})
     		.lines
     		.forEach(function(line) {
         				lineCnt ++;
         				if(lineCnt >= dataPartition.from && lineCnt < dataPartition.to)
         				obj.data.push(line.toString());
     				});
}

RddWorker.prototype.loadHDFS = function(dataPartition, cb) {
	// TODO
}


RddWorker.prototype.applyLinearTrans = function(trans) {	
	var obj = this;
	var data = this.data;
	trans.forEach(function(f){
		data = obj.applyTrans(data, f);
	});
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

/** 
* actions. requested data should have already been loaded in memory.
*/
RddWorker.prototype.count = function(partition) {
	console.log(partition);
	return this.data.length;
}

RddWorker.prototype.collect = function(partition) {
	console.log(partition);
	return this.data;
}

exports.RddWorker = RddWorker;
