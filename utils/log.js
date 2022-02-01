const { DateTime } = require('luxon');
const { executeQueryFromNode } = require('../db');

async function getEventLogs(node, timestamp = '1970-01-01 00:00:00.000') {
    const query = `select * from movie_change_events where timestamp > '${timestamp}'`;

    const results = await executeQueryFromNode(node, query);

    return results.map((data) => ({
        ...data,
        timestamp: DateTime.fromJSDate(data.timestamp).toISO({
            includeOffset: false,
        }),
    }));
}

module.exports = { getEventLogs };
