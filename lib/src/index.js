(function () {
    var reader = require('@theoryofnekomata/kanjidic-reader'),
        isPlainObject = require('is-plain-object'),
        importToSqlDatabase = require('./import-sql'),
        importToNoSqlDatabase = require('./import-nosql');

    module.exports = {
        'import': function importData(inputPath, model) {
            return function importer(options, cb) {
                if (!isPlainObject(options)) {
                    throw new Error('Options should be a plain object.');
                }

                return reader(inputPath, data => {
                    try {
                        return importToSqlDatabase(data, model, options, cb || (() => {}));
                    } catch(e) {
                    }

                    throw new Error('Unknown database.');

                    try {
                        return importToNoSqlDatabase(data, options, cb || (() => {}));
                    } catch(e) {
                    }

                    throw new Error('Unknown database.');
                });
            };
        }
    };
})();
