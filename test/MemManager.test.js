var MemManager = require("../MemManager.js");
var os = require('os');
var LRU = require('lru-cache');
var futil = require('../functional_util.js');
var fs = require('fs');

describe('MemManager', function() {
	it('should detect memory usage', function() {
		var options =  { 
			max: 10,
			length: function (n) { return n.length; },
			dispose: function (key, n) { }
		};

		var cache = LRU(options);

		cache.set("key1", "aaaaa");
		cache.set("key2", "bbbbb");

		console.log(cache.values());
		cache.set("key3", "tttttt");
		console.log(cache.values());

		console.log(os.totalmem());
		console.log(os.freemem());
	});

	it('should be able to read file line by line', function() {
		var data = {path : './somefile', start : 2, end : 5};
		var liner = new futil.Liner();
		var source = fs.createReadStream(data.path);
		source.pipe(liner);

		var lineCnt = 0;
		var str = '';
		var ended = false;

		liner.on('readable', function () {
			if(ended) return;

			var line = '';
			while (line = liner.read()) {
				lineCnt++;
				if(lineCnt >= data.start && lineCnt < data.end)
					str += line + '\n';
				if(lineCnt >= data.end) {
					ended = true;
					source.close();
					console.log('line-by-line ' + str);
					break;
				}
			};
		});	
	});

	it('should return correct bytes when loading by bytes', function() {
		var data = {content : './somefile', start : 2, end : 50};
		var source = fs.createReadStream(data.content, {start : data.start, end : data.end - 1});
		var chunks = [];
		source.on('data', function(chunk) {
			chunks.push(chunk);
		});

		source.on('end', function() {
			console.log(Buffer.concat(chunks).toString(data.encoding || 'utf8'));
		})
	});

	it('should return correct result', function() {
		var mem = new MemManager(100 * 1024 * 1024);
		mem.loadFromLocalFile({path : './somefile', splitBy : 'line', start : 1, end : 3, encoding : 'utf8'},
			function(err, key, value) { 
				if(err) {
					console.log(err);
				} else {
					console.log('key = ' + key); 
					console.log('value = ' + value);
				}
			});
	})
});
