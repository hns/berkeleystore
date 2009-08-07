addToClasspath('/usr/share/java/libdb4.6-java.jar')

require('core/string');
include('core/json');
include('./storeutils');

var log = require('helma/logging').getLogger(module.id);
var __shared__ = true;

importPackage(com.sleepycat.db);
importPackage(com.sleepycat.bind.tuple)

export("Store", "Storable", "Transaction");
var Storable = require('./storable').Storable;
Storable.setStoreImplementation(this);

var ALLOW_CREATE = true;
var TRANSACTIONAL = true;
var SHARED_CACHE = true;
var CACHE_SIZE = 1 * 1024 * 1024;
var SUCCESS = OperationStatus.SUCCESS;

var datastore = new Store("db");

var EQUAL = 0;
var GREATER_THAN = 1;
var GREATER_THAN_OR_EQUAL = 2;
var LESS_THAN = 3;
var LESS_THAN_OR_EQUAL = 4;

exports.init = function(location) {
    if (datastore) {
        datastore.close();
    }
    datastore = new Store(location);
    java.lang.Runtime.runtime.addShutdownHook(new JavaAdapter(java.lang.Thread, {
        run: function() {
            exports.close();
        }
    }));
}

exports.close = function() {
    if (datastore) {
        datastore.close();
        datastore = null;
    }
}

function Store(location) {
    var env = openEnvironment(location);
    var seq = openDatabase(env, 'data.db', '__idgen__');
    var tables = [];
    // var cache = {};
    var store = this;

    function Table(type) {
        var db = openDatabase(env, 'data.db', type);
        var index = openIndex(db, 'index.db', type);
        var idgen = openSequence(seq, type);
        var table = this;

        function entryToEntity(id, data) {
            var entity = JSON.parse(entryToString(data));
            Object.defineProperty(entity, "_key", {
                value: createKey(type, id)
            });
            return entity;
        }

        function getStats(database) {
            // var statsconf = new StatsConfig();
            // statsconf.setFast(true);
            return database.getStats(null, null)
        }

        this.load = function(id) {
            var data = new DatabaseEntry();
            var key = numberToEntry(id);
            if (db.get(null, key, data, null) == SUCCESS) {
                return entryToEntity(id, data);
            } else {
                return null;
            }
        };

        this.store = function(entity, txn) {
            var id = getId(entity._key);
            db.put(null, numberToEntry(id), stringToEntry(JSON.stringify(entity)));
        };

        this.remove = function(id, txn) {
            db["delete"](null, numberToEntry(id));
        };

        this.retrieve = function(id) {
            var entity = this.load(id);
            if (entity) {
                return new Storable(type, entity);
            }
            return null;
        };

        this.retrieveAll = function() {
            var cursor = db.openCursor(null, null);
            var key = new DatabaseEntry();
            var data = new DatabaseEntry();
            data.setPartial(0, 0, true);
            var status = cursor.getFirst(key, data, null);
            var list = [];

            while (status === SUCCESS) {
                // list.push(new Storable(type, entryToEntity(entryToNumber(key), data)));
                list.push(new Storable(type, createKey(type, entryToNumber(key))));
                status = cursor.getNext(key, data, null);
            }
            tryClose(cursor);            
            return list;
        };

        function QueryImpl() {

            var filters = [];

            function getComparableValue(value) {
               if (isStorable(value)) {
                    return value._key.$ref;
                } else if (value instanceof Date || value instanceof java.util.Date) {
                    return value.getTime();
                }
                return value;
            }

            this.addFilter = function(name, operator, value) {

                var comparable = getComparableValue(value);

                filters.push({
                    getWeight: function(cursor, key, data, count) {
                        if (operator != EQUAL) {
                            return 0;
                        }
                        if (isNaN(this.weight)) {
                            this.initIndexKey(key);
                            if (cursor.getSearchKey(key, data, null) != SUCCESS) {
                                return -5;
                            }
                            var ratio = cursor.count() / count;
                            this.weight = Math.round((ratio - 0.5) * 10);
                        }
                        return this.weight;
                    },
                    initIndexKey: function(key, omitValue) {
                       propertyToIndexKey(name, value, key, omitValue || operator >= LESS_THAN);
                    },
                    initCursor: function(cursor, key, pkey, data) {
                        return operator == EQUAL ?
                               cursor.getSearchKey(key, pkey, data, null) :
                               cursor.getSearchKeyRange(key, pkey, data, null);
                    },
                    initCursorBoth: function(cursor, key, pkey, data) {
                        return operator == EQUAL ?
                               cursor.getSearchBoth(key, pkey, data, null) :
                               cursor.getSearchBothRange(key, pkey, data, null);
                    },
                    checkObject: function(s) {
                        if (s) {
                            var value = getComparableValue(s[name]);
                            return this.checkProperty([name, value]);
                        }
                        return false;
                    },
                    checkProperty: function([n, v]) {
                        if (n != name) return false;
                        switch (operator) {
                            case EQUAL:
                                return v == comparable;
                            case LESS_THAN:
                                return v < comparable;
                            case LESS_THAN_OR_EQUAL:
                                return v <= comparable;
                            case GREATER_THAN:
                                return v == comparable ? 0 : v > comparable;
                            case GREATER_THAN_OR_EQUAL:
                                return v >= comparable;
                        }
                    }
                });
            }

            this.select = function(property) {
                var stats = getStats(db);
                var count = stats.getNumKeys();
                var key = new DatabaseEntry();
                var pkey = new DatabaseEntry();
                var data = new DatabaseEntry();
                data.setPartial(0, 0, true);
                var cursor = index.openCursor(null, null);
                // query optimization - sort filters according to the number of matched objects
                filters.sort(function(a, b) {
                    return a.getWeight(cursor, key, data, count) - b.getWeight(cursor, key, data, count);
                });
                var results;
                for (let i = 0; i < filters.length; i++) {
                    var filter = filters[i];
                    if (i == 0 || results.count > count / 4) {
                        var previousResults = results;
                        results = {}
                        Object.defineProperty(results, 'count', {value: 0, writable: true});
                        filter.initIndexKey(key);
                        let status = filter.initCursor(cursor, key, pkey, data);
                        if (status != SUCCESS) {
                            results = {};
                            break;
                        }
                        while(status == SUCCESS) {
                            let s = filter.checkProperty(indexKeyToProperty(key));
                            if (s) {
                                let id = entryToNumber(pkey);
                                if (!previousResults || id in previousResults) {
                                    results[id] = true;
                                    results.count += 1;
                                }
                            } else if (s === false) {
                                break;
                            }
                            status = cursor.getNext(key, pkey, data, null);
                        }
                    } else {
                        for (let id in results) {
                            if (!id) continue;
                            var s = table.retrieve(id);
                            if (!filter.checkObject(s)) {
                                delete results[id];
                                results.count -= 1;
                            }
                        }
                    }
                }
                tryClose(cursor);
                var list = [];
                for (let id in results) {
                    if (property == '_id') {
                        list.push(id);
                    } else {
                        var s = new Storable(type, createKey(type, id));
                        list.push(property ? s[property] : s);
                    }
                }
                return list;
            };
        }

        function OperatorQuery(parentQuery, operator, name, value) {

            var q = Object.create(BaseQuery.prototype);

            q.select = function(property) {
                return this.getQuery().select(property);
            };

            q.getQuery = function() {
                var query = parentQuery.getQuery();
                query.addFilter(name, operator, value);
                return query;
            };

            return q;
        }

        function BaseQuery() {

            var q = Object.create(BaseQuery.prototype);

            q.select = function(property) {
                var cursor = db.openCursor(null, null);
                var key = new DatabaseEntry();
                var data = new DatabaseEntry();
                if (property === '_id') {
                    // if we're only interested in ids don't retrieve data
                    data.setPartial(0, 0, true);
                }
                var results = [];
                while (cursor.getNext(key, data, null) === SUCCESS) {
                    var id = entryToNumber(key);
                    if (property === '_id') {
                        results.push(id);
                    } else {
                        var s = new Storable(type, entryToEntity(id, data));
                        results.push(property ? s[property] : s);
                    }
                }
                tryClose(cursor);
                return results;
            };

            q.getQuery = function() {
                return new QueryImpl();
            };

            return q;
        }

        BaseQuery.prototype.equals = function(name, value) {
            return OperatorQuery(this, EQUAL, name, value);
        };

        BaseQuery.prototype.greater = function(name, value) {
            return OperatorQuery(this, GREATER_THAN, name, value);
        };

        BaseQuery.prototype.greaterEquals = function(name, value) {
            return OperatorQuery(this, GREATER_THAN_OR_EQUAL, name, value);
        };

        BaseQuery.prototype.less = function(name, value) {
            return OperatorQuery(this, LESS_THAN, name, value);
        };

        BaseQuery.prototype.lessEquals = function(name, value) {
            return OperatorQuery(this, LESS_THAN_OR_EQUAL, name, value);
        };

        this.query = BaseQuery;

        this.generateId = function() {
            return idgen.get(null, 1);
        }

        this.close = function() {
            tryClose(db);
            tryClose(index);
            tryClose(idgen);
        }
    }

    function getTable(type) {
        var table = tables[type];
        if (!table) {
            table = tables[type] = new Table(type);
        }
        return table;
    }

    this.load = function(type, id) {
        return getTable(type).load(id);
    }

    this.store = function(entity, txn) {
        var type = getType(entity._key);
        getTable(type).store(entity, txn);
    }

    this.query = function(type) {
        return getTable(type).query();
    }

    this.remove = function(key, txn) {
        var [type, id] = key.$ref.split(":");
        getTable(type).remove(id, txn);
    }

    this.retrieve = function(type, id) {
        return getTable(type).retrieve(id);
    }

    this.retrieveAll = function(type, id) {
        return getTable(type).retrieveAll(id);
    }

    this.search = function(type, name, value) {
        getTable(type).search(name, value);
    }

    this.generateId = function(type) {
        return getTable(type).generateId();
    }

    this.close = function() {
        for (var i in tables) {
            tryClose(tables[i]);
        }
        tryClose(seq);
        tryClose(env);
        seq = env = null;
        tables = [];
    }
}

exports.search = function(type, name, value) {
    datastore.search(type, name, value);
}

function all(type) {
    return datastore.retrieveAll(type);
}

function get(type, id) {
    return datastore.retrieve(type, id);
}

function query(type) {
    return datastore.query(type);
}

function remove(key, txn) {
    datastore.remove(key, txn);
}

function save(props, entity, txn) {
    var wrapTransaction = false;
    if (!txn) {
        txn = new BaseTransaction();
        wrapTransaction = true;
    }

    if (updateEntity(props, entity, txn)) {
        datastore.store(entity, txn);
        if (wrapTransaction) {
            // txn.commit();
        }
    }
}

function getEntity(type, arg) {
    if (isKey(arg)) {
        var [type, id] = arg.$ref.split(":");
        return datastore.load(type, id);
    } else if (isEntity(arg)) {
        return arg;
    } else if (arg instanceof Object) {
        var entity = arg.clone({});
        Object.defineProperty(entity, "_key", {
            value: createKey(type, datastore.generateId(type))
        });
        return entity;
    }
    return null;
}

function openEnvironment(location, options) {
    var envconf = new EnvironmentConfig();
    applyOptions(envconf, options || {
        allowCreate: ALLOW_CREATE,
        cacheSize: CACHE_SIZE,
        transactional: TRANSACTIONAL,
        initializeCache: TRANSACTIONAL,
        initializeLocking: TRANSACTIONAL,
        initializeLogging: TRANSACTIONAL
    });
    return new Environment(new java.io.File(location), envconf);
}

function openDatabase(env, location, name, options) {
    var dbconf = new DatabaseConfig();
    applyOptions(dbconf, options || {
        allowCreate: ALLOW_CREATE,
        transactional: TRANSACTIONAL,
        type: DatabaseType.BTREE
    });
    return env.openDatabase(null, location, name, dbconf);
}

function openIndex(db, location, name, options) {
    var dbconf = new SecondaryConfig();
    applyOptions(dbconf, options || {
        allowCreate: ALLOW_CREATE,
        transactional: TRANSACTIONAL,
        sortedDuplicates: true,
        type: DatabaseType.BTREE,
        multiKeyCreator: new SecondaryMultiKeyCreator({
            createSecondaryKeys: function(secondary, primaryKey, primaryData, results) {
                var obj = JSON.parse(entryToString(primaryData));
                for (var prop in obj) {
                    var value = obj[prop];
                    if (isIndexableProperty(prop, value)) {
                        if (value instanceof Array) {
                            for (var i = 0; i < value.length; i++) {
                                if (isIndexableProperty(value[i]) && !(value[i] instanceof Array)) {
                                    results.add(propertyToIndexKey(prop, value[i]));
                                }
                            }
                        } else {
                            results.add(propertyToIndexKey(prop, value));
                        }
                    }
                }
            }
        })
    });
    return db.environment.openSecondaryDatabase(null, location, name + "__index", db, dbconf);
}

function openSequence(db, name, options) {
    var seqconf = new SequenceConfig();
    applyOptions(seqconf, options || {
        allowCreate: ALLOW_CREATE
    })
    return db.openSequence(null, stringToEntry(name), seqconf);
}

function tryClose(r) {
    try {
        r.close();
    } catch (error) {
        log.error(error);
    }
}

function equalValues(value1, value2) {
    return value1 === value2 || isKey(value1) && isKey(value2) && equalKeys(value1, value2);
}

function applyOptions(r, options) {
    for (var prop in options) {
        r[prop] = options[prop];
    }
}

function stringToEntry(str, entry) {
   entry = entry || new DatabaseEntry();
   StringBinding.stringToEntry(str, entry);
   return entry;
};

function numberToEntry(num, entry) {
    entry = entry || new DatabaseEntry();
    IntegerBinding.intToEntry(num, entry);
    return entry;
}

var entryToString = StringBinding.entryToString;
var entryToNumber = IntegerBinding.entryToInt;

var TYPE_NULL = 0;
var TYPE_STRING = 1;
var TYPE_NUMBER = 2;
var TYPE_BOOLEAN = 3;
var TYPE_DATE = 4;
var TYPE_REFERENCE = 5;

// Binding to convert object properties to index keys and vice versa
function propertyToIndexKey(name, value, entry, omitValue) {
    var output = new TupleOutput();
    output.writeString(String(name));
    if (typeof value == 'string') {
        output.writeByte(TYPE_STRING);
        if (!omitValue) output.writeString(value);
    } else if (typeof value == 'number') {
        output.writeByte(TYPE_NUMBER);
        if (!omitValue) output.writeSortedDouble(value);
    } else if (typeof value == 'boolean') {
        output.writeByte(TYPE_BOOLEAN);
        if (!omitValue) output.writeBoolean(value);
    } else if (isStorableDate(value)) {
        output.writeByte(TYPE_DATE);
        if (!omitValue) output.writeLong(value.$timestamp);
    } else if (value instanceof Date || value instanceof java.util.Date) {
        output.writeByte(TYPE_DATE);
        if (!omitValue) output.writeLong(value.getTime());
    } else if (isKey(value)) {
        output.writeByte(TYPE_REFERENCE);
        if (!omitValue) output.writeString(value.$ref);
    } else {
        output.writeByte(TYPE_NULL);
    }
    entry = entry || new DatabaseEntry();
    TupleBase.outputToEntry(output, entry);
    return entry;
}

function indexKeyToProperty(entry) {
    var input = TupleBase.entryToInput(entry);
    var name = input.readString();
    var proptype = input.readByte();
    var value;
    switch (proptype) {
        case TYPE_STRING:
            value = input.readString();
            break;
        case TYPE_NUMBER:
            value = input.readSortedDouble();
            break;
        case TYPE_BOOLEAN:
            value = input.readBoolean();
            break;
        case TYPE_DATE:
            value = input.readLong();
            break;
        case TYPE_REFERENCE:
            value = input.readString();
            break;
    }
    return [name, value];
}

function isIndexableProperty(name, value) {
    if (typeof(value) === 'string') {
        return value.length <= 200;
    }
    if (typeof(value) === 'object') {
        return value == null || value instanceof Array || isKey(value) || isStorableDate(value);
    }
    return true;
}
