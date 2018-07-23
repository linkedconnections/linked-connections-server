const Writable = require('stream').Writable;
const fs = require('fs');

module.exports = class pageWriterStream extends Writable {

  constructor(targetPath, size) {
    super({ objectMode: true });
    this._targetPath = targetPath + '/';
    this._size = size;
    this._byteCount = 0;
    this._currentFileName = '';
    this._wstream = '';
    this._lastDepartureTime = null;
  }

  _write(data, encoding, done) {
    let dataString = JSON.stringify(data);
    let buffer = Buffer.from(dataString);

    if (this._currentFileName == '') {
      this._currentFileName = data.departureTime;
      this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
      this._wstream.write(dataString);
      this._byteCount += buffer.byteLength;
    } else {
      if (this._byteCount >= this._size && data.departureTime != this._lastDepartureTime) {
        this._wstream.end();
        this._currentFileName = data.departureTime;
        this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
        this._wstream.write(dataString);
        this._byteCount = buffer.byteLength;
      } else {
        this._wstream.write(',\n' + dataString);
        this._byteCount += buffer.byteLength;
      }
    }
    this._lastDepartureTime = data.departureTime;
    done();
  }
}

/*module.exports = class pageWriterStream extends Writable {

  constructor(targetPath) {
    super({ objectMode: true });
    this._targetPath = targetPath + '/';
    this._currentFileName = '';
    this._wstream = '';
  }

  _write(data, encoding, callback) {
    var rounded_date = roundDate(data.departureTime);
    
    if (this._currentFileName == '') {
      this._currentFileName = rounded_date.toISOString();
      this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
      this._wstream.write(JSON.stringify(data)); 
    } else {
      if (rounded_date.toISOString() !== this._currentFileName) {
        this._wstream.end();
        this._currentFileName = rounded_date.toISOString();
        this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
        this._wstream.write(JSON.stringify(data));
      } else {
        this._wstream.write(',\n' + JSON.stringify(data));
      }
    }
    callback();
  }

}

// TODO: Make fragmentation configurable
function roundDate(date_text) {
  var date = new Date(date_text);
  var round_down = date.getMinutes() - (date.getMinutes() % 10);
  date.setMinutes(round_down);
  date.setSeconds(0);
  return date;
}*/