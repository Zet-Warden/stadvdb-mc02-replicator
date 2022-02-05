const { DateTime } = require('luxon');
const { executeQueryFromNode } = require('../db');
const {
    startReplicationTransaction,
    commitReplicationTransaction,
    rollbackReplicationTransaction,
} = require('./transaction');
const { getEventLogs } = require('./log');

async function syncLogEvents(publisher, logEvents) {
    for (log of logEvents) {
        const targetNodes = log.node_target.split(',');
        const data = {
            id: log.id,
            name: log.name,
            year: log.year,
            rank: log.rank,
        };

        for (node of targetNodes) {
            await sync(node, log.type, publisher, log.timestamp, data);
        }
    }
}

async function syncNodeFromNode(subscriber, publisher) {
    const { last_update: latestUpdateTime } = await getLastestUpdates(
        subscriber,
        publisher
    );

    const eventLogs = await getEventLogs(publisher, latestUpdateTime);

    //get only event logs from the publisher that is targeted for the subscriber
    const subscriberOnlyEventLogs = eventLogs.filter((log) =>
        log.node_target.split(',').includes(subscriber)
    );

    await syncLogEvents(publisher, subscriberOnlyEventLogs);
}

async function getLastestUpdates(node, publisher) {
    const latestUpdatesQuery = `   select * from _v 
                                    where publisher = '${publisher}'`;
    const [getLatestUpdates] = await executeQueryFromNode(
        node,
        latestUpdatesQuery
    );

    return {
        ...getLatestUpdates,
        last_update: DateTime.fromJSDate(getLatestUpdates.last_update).toISO({
            includeOffset: false,
        }),
    };
}

async function sync(node, type, publisher, publisherLastUpdate, data) {
    const query = getSyncQuery(type, data);

    try {
        //create transaction
        var conn = await startReplicationTransaction(node);
        await conn.query(query);
        await commitReplicationTransaction(conn);

        //update version of db after transaction
        await updateVersion(node, publisher, publisherLastUpdate);
        console.log(`${node}: ${type} sync successful`);
    } catch (err) {
        //do error handling i.e. when node is unavailable
        await rollbackReplicationTransaction(conn);
        console.log(err);
    }
}

function getSyncQuery(type, { id, name, year, rank }) {
    //prevent mysql errors from strins w/ ' or "
    const cleanName = name.replaceAll("'", "\\'").replaceAll('"', '\\"');
    if (type === 'UPDATE') {
        var query = `   update movies
                        set name = '${cleanName}',
                        year = ${year},
                        \`rank\` = ${rank}
                        where id = ${id};`;
    } else if (type === 'DELETE') {
        var query = `   delete from movies
                        where id = ${id};`;
    } else if (type === 'INSERT') {
        var query = `   insert into movies(id, name, year, \`rank\`)
                        values (${id}, '${cleanName}', ${year}, ${rank})`;
    }

    return query;
}

async function updateVersion(node, publisher, publisherLastUpdate) {
    const updateVersion = ` update _v
                            set last_update = '${publisherLastUpdate}'
                            where publisher = '${publisher}'`;
    await executeQueryFromNode(node, updateVersion);
}

module.exports = { syncNodeFromNode };
