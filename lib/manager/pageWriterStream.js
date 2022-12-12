const Writable = require('stream').Writable;
const fs = require('fs');
const zlib = require('zlib');

module.exports = class pageWriterStream extends Writable {

    constructor(targetPath, size) {
        super({ objectMode: true });
        this._targetPath = targetPath + '/';
        this._size = size;
        this._count = 0;
        this._currentFileName = null;
        this._lastDepartureTime = null;
        this._gzip = null;
        this._wstream = null;
    }

    _write(data, encoding, done) {
        const dataValue = data.value;

        if (!dataValue['@context']) {
            const dataString = JSON.stringify(dataValue);

            if (!this._currentFileName) {
                this._currentFileName = dataValue.departureTime;
                this.initWriter();
                this._gzip.write(dataString, () => {
                    this._count++;
                    done();
                });

            } else {
                if (this._count >= this._size && dataValue.departureTime != this._lastDepartureTime) {
                    this._gzip.end(null, () => {
                        this._currentFileName = dataValue.departureTime;
                        this.initWriter();
                        this._gzip.write(dataString, () => {
                            this._count = 1
                            this._lastDepartureTime = dataValue.departureTime;
                            done();
                        });
                    });
                } else {
                    this._gzip.write(',\n' + dataString, () => {
                        this._count++;
                        this._lastDepartureTime = dataValue.departureTime;
                        done();
                    });
                }
            }
        } else {
            done();
        }
    }

    _final(done) {
        this._wstream.on('finish', () => { 
            done(); 
        });
        this._gzip.end();
    }

    initWriter() {
        this._gzip = zlib.createGzip();
        this._wstream = this._gzip.pipe(fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld.gz'));
    }
}