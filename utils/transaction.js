const { getConnectionFromNode } = require('../db');

async function startReplicationTransaction(node) {
    const conn = await getConnectionFromNode(node);
    const startTransaction = 'start transaction';
    const setReplicationFlag = 'set @replicator := true';

    await conn.query(startTransaction);
    await conn.query(setReplicationFlag);

    return conn;
}

async function commitReplicationTransaction(conn) {
    const commitTransaction = 'commit';
    const revertReplicationFlag = 'set @replicator := false';

    await conn.query(revertReplicationFlag);
    await conn.query(commitTransaction);

    conn.release();
}

async function rollbackReplicationTransaction(conn) {
    const rollbackTransaction = 'rollback';

    await conn.query(rollbackTransaction);

    conn.release();
}

module.exports = {
    startReplicationTransaction,
    commitReplicationTransaction,
    rollbackReplicationTransaction,
};
