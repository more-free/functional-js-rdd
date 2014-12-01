var os = require("os");
var fs = require('fs');
var md5 = require('MD5');
var futil = require('./functional_util.js');
var memwatch = require('memwatch');

/** 
* LRU based key-value in-memory storage, a naive implementation. 
* @param maxMemInByte
*
*/
function MemManager(maxMemInByte) {
	this.maxMemInByte = maxMemInByte || os.totalmem() * 0.1; 
	this.keys = {};
	this.usageSeq = [];
}

// TODO to support HDFS; to support that even all mem could not tolerate the file; let end = -1 to indicate
// read until the end.
/**
* @param data {path : '/local/file/path', splitBy : 'line', start : 0, end : 10, encoding : 'utf8'}, 
*			  {path : '/local/file/path', splitBy : 'byte', start : 0, end : 1024, encoding : 'utf8'}
*			   note <start> is inclusive while <end> is exclusive.
* @param callback(err, key, value)
*/
MemManager.prototype.loadFromLocalFile = function(data, callback) {
	var key = this.md5Key({type : 'localFile', content : data.path, encoding : data.encoding});
	var obj = this;
	var next = function(err, str) {
		if(!err) 
			obj.put(key, str);
		
		callback(err, key, str);
	};

	switch(data.splitBy) {
		case 'line' :
			this.loadLinesFromLocalFile(data, next);
			break;

		case 'byte' :
			this.loadBytesFromLocalFile(data, next);

		default :
	}
}

MemManager.prototype.loadLinesFromLocalFile = function(data, next) {
	var liner = new futil.Liner();
	var source = fs.createReadStream(data.path);
	source.pipe(liner);

	var lineCnt = 0;
	var str = '';
	var ended = false;
	var maxMem = this.maxMemInByte;

	liner.on('readable', function () {
		if(ended) return;

     	var line = '';
     	while (line = liner.read()) {
     	   // check memory usage
		   if(!ended && process.memoryUsage().rss >= maxMem) {
		   	  next(new Error('no enough memory'), null);
		   	  source.close();
		   	  break;
		   }

           lineCnt++;
           if(lineCnt >= data.start && lineCnt < data.end)
           		str += line + '\n';
           if(lineCnt >= data.end) {
           		ended = true;
           		next(null, str);
           		source.close();
           		break;
           	}
     	};
	});	
}

MemManager.prototype.loadBytesFromLocalFile = function(data, next) {
	var estimatedMem = data.end - data.start + 1;
	if(estimatedMem >= this.maxMemInByte) {
		next(new Error('no enough memory'), null);
		return;
	}

	var source = fs.createReadStream(data.content, {start : data.start, end : data.end - 1});
	var chunks = [];
	source.on('data', function(chunk) {
		chunks.push(chunk);
	});

	source.on('end', function() {
		next(null, Buffer.concat(chunks).toString(data.encoding || 'utf8'));
	});
}

MemManager.prototype.get = function(key) {
	var value = this.keys[key];
	if(value) {
		this.updateUsageSeq(key);
	}

	return value;
}

MemManager.prototype.peek = function(key) {
	return keys[key];
}

MemManager.prototype.exists = function(key) {
	return !!keys[key];
}

MemManager.prototype.put = function(key, value) {
	this.keys[key] = value;
	this.updateUsageSeq(key);
}

/**
* @param cb it will be invoked after the next garbage collection
*/
MemManager.prototype.remove = function(key, cb) {
	delete this[key];
	this.removeFromUsageSeq(key);

	if(cb) {
		memwatch.once('stats', function(stats) {
			cb(stats);
		});
	}
}

MemManager.prototype.removeAll = function(cb) {
	this.keys = {};
	this.usageSeq = [];

	if(cb) {
		memwatch.once('stats', function(stats) {
			cb(stats);
		});
	}
}

MemManager.prototype.freeMem = function() {
	return this.maxMemInByte - process.memoryUsage().rss;
}

/**
* @param data {type : 'str', content : 'some string', encoding : 'utf8'}, 
*			  {type : 'localFile', content : 'some/file/path', encoding : 'utf8'}
*/
MemManager.prototype.md5Key = function(data) {
	switch(data.type) {
		case 'localFile' :
			return md5(this.firstBytesOfLocalFile(data.content, 1024, data.encoding));

		default :
			return md5(data.content);
	}
}

MemManager.prototype.firstBytesOfLocalFile = function(path, bytes, encoding) {
	var fd = fs.openSync(path, 'r');
	var buffer = new Buffer(bytes);
	var read = fs.readSync(fd, buffer, 0, bytes, null);
	fs.closeSync(fd);

	return buffer.toString(encoding || 'utf8', 0, read);
}


MemManager.prototype.updateUsageSeq = function(key) {
	var index = this.usageSeq.indexOf(key);
	if (index >= 0) {
    	this.usageSeq.splice(index, 1);
    	this.usageSeq.unshift(key);
	} else {  // new key
		this.usageSeq.push(key);
	}
}

MemManager.prototype.removeFromUsageSeq = function(key) {
	var index = this.usageSeq.indexOf(key);
	if (index >= 0) 
    	this.usageSeq.splice(index, 1);
}

module.exports = MemManager;