var fs = require('fs');
var futil = require('./functional_util.js');
var MemManager = require('./MemManager');

/**
*  a worker holds an immutable part of data, unless persist() is called.
*/
function RddWorker(maxMemInBytes) {
	this.memManager = new MemManager(maxMemInBytes);
}

/**
* load data into memory (if necessary); apply a series of linear transformations 
* to data, ex. map, filter, flatMap, etc. 
* @param trans { source : xxx, trans : [{xxx}, {xxx}] }
* @param callback(err, [originKey, transformedKey])
*/
RddWorker.prototype.linearTransform = function(trans, callback) {
	var obj = this;
	var cb = function(err, keyInMem) {
		if(err) {
			callback(err, null);
			return;
		}

		try {
			// get new key in memory after applying all linear transformations to the partition.
			// the old partition may still in memory
			var newKey = obj.applyLinearTrans(keyInMem, trans);
			callback(null, [keyInMem, newKey]); 
		} catch(err) {
			callback(err, null);
		}
	};

	if(!trans.source.key || !this.memManager.exists(trans.source.key)) { 
		switch(trans.source.type) {
			case 'hdfs':
				this.loadFromHDFS(trans.source, cb);
				break;
			default :
				this.loadFromLocalFile(trans.source, cb);
		}
	} else {
		var value = this.memManager.get(trans.source.key);
		cb(null, trans.source.key);
	}
}


/**
* @param cb(err, key)
*/
RddWorker.prototype.loadFromLocalFile = function(dataPartition, cb) {
	this.memManager.loadFromLocalFile(
		 {
		 path : dataPartition.path, 
		 splitBy : dataPartition.splitBy, 
		 start : dataPartition.from, 
		 end : dataPartition.to, 
		 encoding : dataPartition.encoding
		 },

		 function(err, key, value) {
		 	cb(err, key);
		 }
		);
}

RddWorker.prototype.loadFromHDFS = function(dataPartition, cb) {
	// TODO
}


/**
* optimize transformation sequence;
* apply transformations;
* remove intermediate RDD partitions to release memory space (maybe implicitly);
* load new value, new key into memManager;
* finally return the new key (for the new partition result, which is computed
* by applying transformations sequentially to the original partition)
*
* @param trans
* @param key key in memManager for the original partition
*/
RddWorker.prototype.applyLinearTrans = function(key, trans) {
	var obj = this;
	// string. already loaded into memory
	var data = [this.memManager.get(key)]; 
	this.optimizeTransSeq(trans.trans).forEach(function(f){
		data = obj.applyTrans(data, f);
	});
	
	// compute key based on the first 1024 chars / elements
	var key = this.memManager.md5Key(
		{
		 type : 'str', 
		 content : data.slice(0, 1024),
		 encoding : trans.source.encoding
		});

	this.memManager.put(key, data);

	return key;
}

// TODO. optimize the transformations. ex. do filter first
RddWorker.prototype.optimizeTransSeq = function(trans) {
	return trans;
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
	var key = partition.transKey;
	var value = this.memManager.get(key);
	var cnt = value.length;

	// clear transformed key. since original key is stored in Rdd driver side, 
	// there is no need to maintain the transformed key
	this.memManager.remove(key);

	return cnt;
}

RddWorker.prototype.collect = function(partition) {
	var key = partition.transKey;
	var value = this.memManager.get(key);

	this.memManager.remove(key);
	return value;
}

exports.RddWorker = RddWorker;
