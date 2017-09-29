const fs = require('fs');
const jsonldstream = require('jsonld-stream');
const zlib = require('zlib');
const pageWriterStream = require('./pageWriterStream.js');
const logger = require('../utils/logger');

module.exports.paginateDataset = function (company_name, target_path, storage) {
  return new Promise((resolve, reject) => {
    try {
      let stream = fs.createReadStream(storage + '/linked_connections/' + company_name + '/' + target_path + '.jsonld.gz')
        .pipe(new zlib.createGunzip())
        .pipe(new jsonldstream.Deserializer())
        .pipe(new pageWriterStream(storage + '/linked_pages/' + company_name + '/' + target_path))
        .on('finish', function () {
          logger.info("Fragmentation process for " + company_name + " completed");
          resolve();
        });
    } catch (err) {
      reject(err);
    }
  });
}


