import { Writable } from 'stream';
import fs from 'fs';
import zlib from 'zlib';

export class PageWriterStream extends Writable {

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
        if (this._wstream) {
            this._wstream.on('finish', () => {
                done();
            });
        }
        if (this._gzip) {
            this._gzip.end();
        }
    }

    initWriter() {
        this._gzip = zlib.createGzip();
        this._wstream = this._gzip.pipe(fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld.gz'));
    }
}