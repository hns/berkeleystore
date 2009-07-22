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
var SUCCESS = OperationStatus.SUCCESS;

var datastore;

exports.init = function(location) {
    datastore = new Store(location);
}

exports.close = function() {
    datastore.close();
    datastore = null;
}

function Store(location) {
    var env = openEnvironment(location);
    var seq = openDatabase(env, 'data.db', '__idgen__');
    var tables = [];

    function Table(type) {
        var db = openDatabase(env, 'data.db', type);
        var index = openIndex(db, 'data.db', type);
        var idgen = openSequence(seq, type);

        function entryToEntity(id, data) {
            var entity = JSON.parse(entryToString(data));
            Object.defineProperty(entity, "_key", {
                value: createKey(type, id)
            });
            return entity;
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
            var status = cursor.getFirst(key, data, null);
            var list = [];

            while (status === SUCCESS) {
                list.push(new Storable(type, createKey(type, entryToNumber(key))));
                status = cursor.getNext(key, data, null);
            }
            tryClose(cursor);            
            return list;
        };

        function EqualsQuery(parent, name, value) {
            // if value is a persistent object we're only interested in the key
            if (isStorable(value)) {
                value = value._key;
            }

            var q = BaseQuery();

            q.select = function(property) {
                var base = parent ? parent.select('_id') : null;
                var cursor = index.openSecondaryCursor(null, null);
                var key = new DatabaseEntry();
                var pkey = new DatabaseEntry();
                var data = new DatabaseEntry();
                if (property === '_id') {
                    // if we're only interested in ids don't retrieve data
                    data.setPartial(0, 0, true);
                }
                var results = [];
                if (base) {
                    for (var i = 0; i < base.length; i++) {
                        propertyToIndexKey(name, value, key);
                        numberToEntry(base[i], pkey);
                        if (cursor.getSearchBoth(key, pkey, data, null) == SUCCESS) {
                            if (property === '_id') {
                                results.push(base[i]);
                            } else {
                                var s = new Storable(type, entryToEntity(base[i], data));
                                results.push(property ? s[property] : s);
                            }

                        }
                    }
                } else {
                    propertyToIndexKey(name, value, key);
                    if (cursor.getSearchKey(key, pkey, data, null) == SUCCESS) {
                        do {
                            var id = entryToNumber(pkey);
                            if (property === '_id') {
                                results.push(id);
                            } else {
                                var s = new Storable(type, entryToEntity(id, data));
                                results.push(property ? s[property] : s);
                            }
                        } while (cursor.getNextDup(key, pkey, data, null) == SUCCESS)
                    }
                }
                tryClose(cursor);
                return results;
            }

            return q;
        }

        function BaseQuery() {

            var q = {};

            q.eq = function(name, value) {
                return EqualsQuery(this, name, value);
            }

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
            }

            return q;
        }

        this.query = BaseQuery;

        this.search = function(name, value) {
            var cursor = index.openSecondaryCursor(null, null);
            var key = propertyToIndexKey(name, value);
            var pkey = new DatabaseEntry();
            var data = new DatabaseEntry();
            // data.setPartial(0, 0, true);
            var status = value == null ?
                         cursor.getSearchKeyRange(key, pkey, data, null) :
                         cursor.getSearchKey(key, pkey, data, null);
            while (status === SUCCESS) {
                var match = indexKeyToProperty(key);
                if (match[0] != name || value != null && !equalValues(match[1], value)) {
                    break;
                }
                print(match, entryToNumber(pkey) , entryToString(data));
                status = cursor.getNext(key, pkey, data, null);
            }
            tryClose(cursor);            
        }

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
    LongBinding.longToEntry(num, entry);
    return entry;
}

var entryToString = StringBinding.entryToString;
var entryToNumber = LongBinding.entryToLong;

var TYPE_NULL = 0;
var TYPE_STRING = 1;
var TYPE_NUMBER = 2;
var TYPE_BOOLEAN = 3;
var TYPE_DATE = 4;
var TYPE_REFERENCE = 5;

// Binding to convert object properties to index keys and vice versa
function propertyToIndexKey(name, value, entry) {
    var output = new TupleOutput();
    output.writeString(String(name));
    if (typeof value == 'string') {
        output.writeByte(TYPE_STRING);
        output.writeString(value);
    } else if (typeof value == 'number') {
        output.writeByte(TYPE_NUMBER);
        output.writeSortedDouble(value);
    } else if (typeof value == 'boolean') {
        output.writeByte(TYPE_BOOLEAN);
        output.writeBoolean(value);
    } else if (value instanceof Date || value instanceof java.util.Date) {
        output.writeByte(TYPE_DATE);
        output.writeLong(value.getTime());
    } else if (isKey(value)) {
        output.writeByte(TYPE_REFERENCE);
        output.writeString(value.$ref);
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
            value = new Date(input.readLong());
            break;
        case TYPE_REFERENCE:
            value = { $ref: input.readString() };
            break;
    }
    return [name, value];
}

function isIndexableProperty(name, value) {
    if (typeof(value) === 'string') {
        return value.length <= 200;
    }
    if (typeof(value) === 'object') {
        return value instanceof java.util.Date
               || (!(value instanceof java.lang.Object)
                    && (isKey(value) || value == null || value instanceof Array));
    }
    return true;
}
