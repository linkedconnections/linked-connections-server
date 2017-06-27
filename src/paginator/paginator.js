var fs = require('fs');
var jsonldstream = require('jsonld-stream');
var pageWriterStream = require('./pageWriterStream.js');
const zlib = require('zlib');

module.exports.paginateDataset = function(company_name, target_path, storage, cb) {
  var t0 = new Date().getTime();

  var stream = fs.createReadStream(storage + '/linked_connections/' + company_name + '/' + target_path + '.jsonld.gz')
    .pipe(new zlib.createGunzip())
    .pipe(new jsonldstream.Deserializer())
    .pipe(new pageWriterStream(storage + '/linked_pages/' + company_name + '/' + target_path))
    .on('finish', function () {
      var t1 = new Date().getTime();
      var tf = (t1 - t0) / 1000;
      console.log("Pagination process took " + tf + " seconds to complete");
      cb();
    });
}


