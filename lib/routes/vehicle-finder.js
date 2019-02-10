var utils = require('../utils/utils');
const fs = require('fs');
const Logger = require('../utils/logger');
const server_config = utils.serverConfig;
const storage = utils.datasetsConfig.storage;
const logger = Logger.getLogger(server_config.logLevel || 'info');

/**
 * @brief Retrieves all vehicles of a certain agency, sorted by departure time.
 * @author Dylan Van Assche
 */
class VehicleFinder {
    /**
     * Returns all the trips depending on the requested departure time.
     * @param req
     * @param res
     * @returns {Promise<void>}
     */
    async getTrips(req, res) {
        logger.debug('Finding trips...');

        // Allow requests from different hosts
        const agency = req.params.agency;
        logger.debug('Agency: ' + agency);

        // Check if agency exist
        let lp_path = storage + '/vehicles/' + agency;
        if (!fs.existsSync(lp_path)) {
            res.status(404).send({
                error: 404,
                message: "Agency not found!"
            });
            return;
        }

        // Send back the vehicles
        let readable = fs.createReadStream(lp_path + '/tree/vehicleTree.jsonld');
        readable.pipe(res);
    }
}

// Export classes
module.exports = VehicleFinder;