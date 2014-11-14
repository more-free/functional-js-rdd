var http = require('http');

// An object of options to indicate where to post to
var post_options = {
      host: 'localhost',
      port: '3000',
      path: '/',
      method: 'POST',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': 0 // change outside
      }
};

// Set up the request
var postJson = function(jsonObj) {
  var post_data = JSON.stringify(closureCleaner(jsonObj));
  post_options.headers['Content-Length'] = post_data.length;

  var post_req = http.request(post_options, function(res) {
  var str = '';
    res.setEncoding('utf8');
    res.on('data', function (chunk) {
        str += chunk;
    });
    res.on('end', function(){
      console.log(str);
    });
  });

  post_req.write(post_data);
  post_req.end();
}


var closureCleaner = function(jsonObj) {
  var newJson = {};
  for(var k in jsonObj) {
    if(typeof jsonObj[k] === 'function')
      newJson[k] = jsonObj[k].toString();
    else
      newJson[k] = jsonObj[k];
  }

  return newJson;
} 


function RddDriver() {

}

RddDriver.prototype.load = function(path) {
  postJson({ type : 'load', url : './somefile'} );
}

RddDriver.prototype.map = function() {

}

RddDriver.prototype.flatMap = function() {
  postJson({ 
    type : 'flatMap', 
    func : function(s) { 
      return s.split(/\s+/); 
    } 
  });
}

RddDriver.prototype.count = function() {
  postJson({ type : 'count' });
}

// test routine :
var driver = new RddDriver();
driver.load();
driver.flatMap();
driver.count();

