const { syncNodeFromNode } = require('./utils/sync');
const nodes = [
    {
        name: 'NODE 1',
        publishers: ['NODE 2', 'NODE 3'],
        subscribers: ['NODE 2', 'NODE 3'],
    },
    {
        name: 'NODE 2',
        publishers: ['NODE 1', 'NODE 2', 'NODE 3'],
        subscribers: ['NODE 1', 'NODE 2', 'NODE 3'],
    },
    {
        name: 'NODE 3',
        publishers: ['NODE 1', 'NODE 2', 'NODE 3'],
        subscribers: ['NODE 1', 'NODE 2', 'NODE 3'],
    },
];

let isSyncing = false;

async function syncNodes() {
    const syncProcesses = [];
    for (node of nodes) {
        for (publisher of node.publishers) {
            const syncProcess = syncNodeFromNode(node.name, publisher);
            syncProcesses.push(syncProcess);
        }
    }
    await Promise.all(syncProcesses);
    isSyncing = false;
}

async function startSync() {
    if (!isSyncing) {
        isSyncing = true;

        await syncNodes();
    } else {
        console.log(
            'Syncer is busy syncing nodes. Please sync again at a later time.'
        );
    }
}

setInterval(startSync, 1000);
// startSync();
// syncNodeFromNode('NODE 2', 'NODE 1');
