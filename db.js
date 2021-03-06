require('dotenv').config();

const mysql = require('mysql2/promise');
const dbConfig = {
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.SCHEMA,
    waitForConnections: true,
    connectionLimit: 100,
    queueLimit: 0,
};

const pool1 = mysql.createPool({
    host: process.env.NODE1,
    ...dbConfig,
});

const pool2 = mysql.createPool({
    host: process.env.NODE2,
    ...dbConfig,
});

const pool3 = mysql.createPool({
    host: process.env.NODE3,
    ...dbConfig,
});

async function getConnectionFromNode(node) {
    try {
        switch (node) {
            case 'NODE 1':
                return await pool1.getConnection();
            case 'NODE 2':
                return await pool2.getConnection();
            case 'NODE 3':
                return await pool3.getConnection();

            default:
                throw 'Node specified cannot be identified.';
        }
    } catch (err) {
        console.log(`${node} UNAVAILABLE`);
        throw err;
    }
}

async function executeQueryFromNode(node, query) {
    try {
        switch (node) {
            case 'NODE 1':
                return (await pool1.query(query))[0];
            case 'NODE 2':
                return (await pool2.query(query))[0];
            case 'NODE 3':
                return (await pool3.query(query))[0];

            default:
                throw 'Node specified cannot be identified.';
        }
    } catch (err) {
        console.log(`${node} UNAVAILABLE`);
        throw err;
    }
}

module.exports = { getConnectionFromNode, executeQueryFromNode };
