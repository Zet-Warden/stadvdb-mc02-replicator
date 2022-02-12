const { DateTime } = require('luxon');
const { executeQueryFromNode } = require('../db');
const {
    startReplicationTransaction,
    commitReplicationTransaction,
    rollbackReplicationTransaction,
} = require('./transaction');
const { getEventLogs } = require('./log');

async function syncLogEvents(subscriber, publisher, logEvents) {
    for (log of logEvents) {
        const data = {
            id: log.id,
            node: subscriber,
            //prevent mysql errors from strins w/ ' or "
            name: log.name.replaceAll("'", "\\'").replaceAll('"', '\\"'),
            year: log.year,
            rank: log.rank,
            oldName: log.old_name,
            oldYear: log.old_year,
            oldRank: log.old_year,
        };

        await sync(subscriber, log.type, publisher, log.timestamp, data);
    }
}

async function syncNodeFromNode(subscriber, publisher) {
    try {
        const { last_update: latestUpdateTime } = await getLastestUpdates(
            subscriber,
            publisher
        );

        const eventLogs = await getEventLogs(publisher, latestUpdateTime);

        //get only event logs from the publisher that is targeted for the subscriber
        const subscriberOnlyEventLogs = eventLogs.filter(
            filterNode(subscriber, publisher)
        );
        await syncLogEvents(subscriber, publisher, subscriberOnlyEventLogs);
    } catch (err) {
        console.log(`${publisher} UNAVAILABLE`);
        console.log('--------------------------');
        console.log(err);
    }
}

function filterNode(node, publisher) {
    switch (node) {
        case 'NODE 1':
            return (log) => {
                return true;
            }; //no filter
        case 'NODE 2':
            return (log) => {
                if (publisher == 'NODE 2') {
                    return log.year >= 1980;
                } else {
                    return log.old_year < 1980 || log.year < 1980;
                }
            };
        case 'NODE 3':
            return (log) => {
                if (publisher == 'NODE 3') {
                    return log.year < 1980;
                } else {
                    return log.old_year >= 1980 || log.year >= 1980;
                }
            };
        default:
            (log) => {
                return true;
            };
    }
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

// let counter = 0;

async function sync(node, type, publisher, publisherLastUpdate, data) {
    const query = getSyncQuery(type, data);
    const queries = query.split(';');
    queries.pop();
    // console.log(queries);

    try {
        //create transaction
        var conn = await startReplicationTransaction(node);
        for (const query of queries) {
            // console.log(query, node);
            await conn.query(query);
        }

        // if (counter < 2) {
        //     counter++;
        //     await conn.query(`SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error for testing transaction failure';
        // `);
        // }

        await commitReplicationTransaction(conn);

        //update version of db after transaction
        await updateVersion(node, publisher, publisherLastUpdate);
        console.log(`${node}: ${type} sync successful`);
    } catch (err) {
        //do error handling i.e. when node is unavailable
        await rollbackReplicationTransaction(conn);
        console.log('Error in syncing:');
        console.log('-------------------------------------------');
        console.log(err);
    }
}

function getSyncQuery(type, data) {
    if (type === 'UPDATE') {
        var query = getUpdateQuery(data);
    } else if (type === 'DELETE') {
        var query = getDeleteQuery(data);
    } else if (type === 'INSERT') {
        var query = getInsertQuery(data);
    }

    return query;
}

function getUpdateQuery(data) {
    let updateQuery = '';
    switch (data.node) {
        case 'NODE 2':
            if (data.year >= 1980) {
                updateQuery += `delete from movies where id = '${data.id}';`;
            } else if (data.oldYear >= 1980) {
                updateQuery += `insert into movies(id, name, year, \`rank\`)
                                values ('${data.id}', '${data.name}', ${data.year}, ${data.rank});`;
            }
            break;

        case 'NODE 3':
            if (data.year < 1980) {
                updateQuery += `delete from movies where id = '${data.id}';`;
            } else if (data.oldYear < 1980) {
                updateQuery += `insert into movies(id, name, year, \`rank\`)
                                values ('${data.id}', '${data.name}', ${data.year}, ${data.rank});`;
            }
            break;
    }

    updateQuery += `update movies
                    set name = '${data.name}',
                    year = ${data.year},
                    \`rank\` = ${data.rank}
                    where id = '${data.id}';`;
    return updateQuery;
}

function getDeleteQuery(data) {
    return `delete from movies
            where id = '${data.id}';`;
}

function getInsertQuery(data) {
    switch (data.node) {
        case 'NODE 2':
            if (data.year >= 1980) {
                return `delete from movies where id = '${data.id}';`;
            }
            break;

        case 'NODE 3':
            if (data.year < 1980) {
                return `delete from movies where id = '${data.id}';`;
            }
            break;
    }

    return `insert into movies(id, name, year, \`rank\`)
            values ('${data.id}', '${data.name}', ${data.year}, ${data.rank});`;
}

async function updateVersion(node, publisher, publisherLastUpdate) {
    const updateVersion = ` update _v
                            set last_update = '${publisherLastUpdate}'
                            where publisher = '${publisher}'`;
    await executeQueryFromNode(node, updateVersion);
}

module.exports = { syncNodeFromNode };
