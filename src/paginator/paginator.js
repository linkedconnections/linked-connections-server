const fs = require('fs');
const jsonldstream = require('jsonld-stream');
const zlib = require('zlib');
const pageWriterStream = require('./pageWriterStream.js');
const logger = require('../utils/logger');

module.exports.paginateDataset = function (source, target_path, company_name, size) {
  return new Promise((resolve, reject) => {
    try {
      let stream = fs.createReadStream(source)
        .pipe(new jsonldstream.Deserializer())
        .pipe(new pageWriterStream(target_path, size))
        .on('finish', function () {
          logger.info("Fragmentation process for " + company_name + " completed");
          resolve();
        });
    } catch (err) {
      reject(err);
    }
  });
}


