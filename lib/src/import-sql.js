(function () {
    var knex = require('knex'),
        get = require('lodash.get'),
        has = require('lodash.has'),
        bluebird = require('bluebird'),
        itemsPerBatch = 128,
        db,
        writeQueue = {};

    function safeName(string) {
        return string.toLowerCase().replace(/[^a-zA-Z0-9_]/g, '_');
    }

    function createColumn(table, safeAttrName, attrType, attrLength) {
        switch (attrType) {
            case 'integer':
                return table.integer(safeAttrName);
            case 'string':
                return table.string(safeAttrName, attrLength);
            default:
                break;
        }

        throw new Error('Error creating column `{safeAttrName}');
    }

    function createColumns(table, obj, parentAttrName, isIndex) {
        Object.keys(obj)
            .forEach(attrName => {
                var safeAttrName = safeName((!!parentAttrName ? parentAttrName + '.' : '') + attrName),
                    typeColonIndex, attrLength, attrType, column;

                if (typeof obj[attrName] === 'object') {
                    return createColumns(
                        table,
                        obj[attrName],
                        (!!parentAttrName ? parentAttrName + '.' : '') + attrName,
                        isIndex || attrName === 'mapping'
                    );
                }

                typeColonIndex = obj[attrName].indexOf(':');
                attrLength = parseInt(typeColonIndex < 0 ? null : obj[attrName].slice(typeColonIndex + 1));
                attrType = obj[attrName].slice(0, typeColonIndex);
                column = createColumn(table, safeAttrName, attrType, attrLength);

                if (!isIndex) {
                    return;
                }

                column.unique();
            });
    }

    function createBaseTable(baseTableName, model) {
        var safeBaseTableName = safeName(baseTableName);

        return db.schema.dropTableIfExists(safeBaseTableName)
            .then(() => db.schema.createTable(safeBaseTableName, (table) => {
                table.increments('id');
                return createColumns(table, model.single);
            }));
    }

    function createJoinTable(baseTableName, entityName, model) {
        var safeBaseTableName = safeName(baseTableName),
            safeEntityName = safeName(entityName),
            joinTableName = `${safeBaseTableName}_${safeEntityName}`;

        writeQueue[entityName] = [];

        return db.schema.dropTableIfExists(joinTableName)
            .then(() => db.schema.createTable(joinTableName, (table) => {
                table.increments('id');
                table.integer(`${safeBaseTableName}_id`, 10)
                    .unsigned()
                    .references(`${safeBaseTableName}.id`);

                createColumns(table, model.multiple[entityName]);
            }));
    }

    function createTables(baseTableName, model) {
        return createBaseTable(baseTableName, model)
            .then(() => bluebird.map(
                Object.keys(model.multiple),
                entityName => createJoinTable(baseTableName, entityName, model)
            ));
    }

    function toBatch(array, maxlength) {
        if (!(array instanceof Array)) {
            throw new Error('Parameter must be array');
        }

        if (isNaN(maxlength) || maxlength < 1) {
            throw new Error('Max length must be greater than zero.');
        }

        maxlength = parseInt(maxlength);

        return array.reduce((batches, item) => {
            if (batches.length < 1 || batches[batches.length - 1].length === maxlength) {
                batches.push([]);
            }

            batches[batches.length - 1].push(item);

            return batches;
        }, []);
    }

    function recursiveFillArray(entry, model, parentAttrName, rows) {
        return rows.map(row => recursiveFill(entry, model, parentAttrName, row));
    }

    function recursiveFill(entry, model, parentAttrName, row) {
        row = row || {};

        if (typeof entry !== 'object') {
            row[safeName(parentAttrName)] = entry;
        } else {
            Object.keys(model).forEach(attr => {
                var item, safeFullAttrName = safeName((!!parentAttrName ? parentAttrName + '.' : '') + attr);

                if (!has(entry, attr)) {
                    return;
                }

                item = get(entry, attr);

                if (typeof item !== 'object') {
                    row[safeFullAttrName] = item;
                    return;
                }

                recursiveFill(item, model[attr], (!!parentAttrName ? parentAttrName + '.' : '') + attr, row);
            });
        }

        return row;
    }

    function insertSingleAttrEntry(entry, tableName, model) {
        return db.table(safeName(tableName))
            .insert(recursiveFill(entry, model.single));
    }

    function queueMultipleAttrEntry(rowId, entry, baseTableName, model) {
        var safeBaseTableName = safeName(baseTableName);

        Object.keys(model.multiple).forEach(entityName => {
            var safeEntityName = safeName(entityName),
                tableName = `${safeBaseTableName}_${safeEntityName}`;

            Object.keys(model.multiple[entityName]).forEach(attrName => {
                var safeAttrName = safeName(attrName);

                if (!has(entry, attrName)) {
                    return;
                }

                get(entry, attrName)
                    .forEach(value => {
                        
                        if (!(value instanceof Array)) {
                            value = [value];
                        }

                        value.forEach(item => {
                            var row = {};
                            row[`${safeBaseTableName}_id`] = rowId;

                            row = recursiveFill(item, model.multiple[entityName][attrName], attrName, row);

                            writeQueue[entityName].push(row);
                        });
                    });
            });
        });

        return;
    }

    function insertEntry(entry, tableName, model) {
        return insertSingleAttrEntry(entry, tableName, model)
            .then((rowIds) => queueMultipleAttrEntry(rowIds[0], entry, tableName, model));
    }

    function insertMultipleAttrEntries(baseTableName, entityName) {
        var safeBaseTableName = safeName(baseTableName),
            safeEntityName = safeName(entityName),
            tableName = `${safeBaseTableName}_${safeEntityName}`;

        return bluebird.mapSeries(
            toBatch(writeQueue[entityName], itemsPerBatch),
            (batch, i, batches) => db.table(tableName).insert(batch)
        );
    }

    function getSqlConfig(options) {
        switch (options.system.toLowerCase()) {
            case 'sqlite':
            case 'sqlite3':
                return {
                    dialect: 'sqlite3',
                    connection: {
                        filename: options.path
                    },
                    useNullAsDefault: true
                };
            case 'maria':
            case 'mariasql':
            case 'mysql':
                return {
                    client: 'mysql',
                    connection: {
                        host: options.host,
                        user: options.user,
                        port: options.port || '3306',
                        password: options.password,
                        database: options.database
                    }
                };
            case 'pg':
            case 'pgsql':
            case 'postgres':
            case 'postgresql':
                return {
                    client: 'pg',
                    connection: {
                        host: options.host,
                        user: options.user,
                        password: options.password,
                        database: options.database
                    }
                };
            default:
                break;
        }

        throw new Error('Unknown SQL client.');
    }

    module.exports = function importToSqlDatabase(data, model, options, cb) {
        db = knex(getSqlConfig(options));

        return createTables(options.tableName, model)
            .then(() => bluebird
                .mapSeries(
                    toBatch(data, itemsPerBatch),
                    (batch, i, batches) => {
                        console.log(`Processing batch ${i + 1} of ${batches}.`);
                        return bluebird.map(batch, entry => insertEntry(entry.data, options.tableName, model));
                    }
                )
            )
            .then(() => bluebird
                .mapSeries(
                    Object.keys(writeQueue),
                    (entityName) => {
                        console.log(`Inserting data in "${entityName}".`);
                        return insertMultipleAttrEntries(options.tableName, entityName);
                    }
                )
            )
            .then(() => db.destroy(cb));
    };
})();
