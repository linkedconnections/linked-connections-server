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
      if (this._byteCount >= this._size) {
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
    done();
  }
}