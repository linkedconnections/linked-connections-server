var utils = require('../utils/utils');
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
    getTrips(req, res) {
        logger.debug('Finding trips...');

        // Allow requests from different hosts
        //res.set({ 'Access-Control-Allow-Origin': '*' }); NOT NEEDED?
        const agency = req.params.agency;
        const headers = { 'Content-Type': 'application/ld+json' };
        logger.debug('Agency: ' + agency);
        res.json({
            test: "OK"
        });
    }
}

// Export classes
module.exports = VehicleFinder;