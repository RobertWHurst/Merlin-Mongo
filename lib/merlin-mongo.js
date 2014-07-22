
// modules
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var path = require('path');
var guard = require('type-guard');
var es = require('event-stream');


/**
 * Merlin NeDB constructor
 * @constructor
 * @param {Merlin} merlin Merlin ORM instance.
 * @param {Object}  opts    Merlin NeDB opts.
 */
function MerlinMongo(merlin, opts) {
  var self = this;

  // validate
  guard('merlin', merlin, 'object');
  guard('opts', opts, 'object');
  guard('opts.databaseUrl', opts.databaseUrl, 'string');

  // setup
  self.merlin = merlin;
  self.opts = opts;
  self.collections = {};
  self.db = null;

  // set merlin config
  self.merlin.opts.idKey = '_id';
  self.merlin.opts.pluralForeignKey = '_{modelName}Ids';
  self.merlin.opts.singularForeignKey = '_{modelName}Id';
  self.merlin.ObjectId = ObjectId;
  self.merlin.Model.staticPrototype.ObjectId = ObjectId;
}

/**
 * Setup the Datastores.
 * @param  {Function} cb Executed upon completion.
 */
MerlinMongo.prototype.connect = function(cb) {
  var self = this;

  // validate
  guard('cb', cb, [ 'function', 'undefined' ]);

  // defaults
  cb = cb || function() {};

  // grab some needed vars
  var dbPath = self.opts.databaseUrl;
  var models = self.merlin.models;

  MongoClient.connect(self.opts.databaseUrl, function(err, db) {
    if(err) { return cb(err); }
    self.db = db;

    // grab all of the collection names
    db.collectionNames(function(err, collectionNames) {
      if(err) { return cb(err); }

      // grab all models registered on merlin so we
      // can create/load a Datastore for each of them.
      var modelNames = Object.keys(models);
      var j = modelNames.length;
      if(j === 0) { return cb(null); }
      for(var i = 0; i < modelNames.length; i += 1) {
        var modelName = modelNames[i];

        var methodName = '';
        if(collectionNames.indexOf(modelName) === -1) {
          methodName = 'createCollection';
        } else {
          methodName = 'collection';
        }
        (function(model) {
          db[methodName](model.collectionName, function(err, collection) {
            if(err && j > 0) { j = 0; return cb(err); }
            self.collections[model.collectionName] = collection;
            j -= 1;
            if(j === 0) { cb(null); }
          });
        })(models[modelName]);
      }
    });
  });

};

/**
 * teardown the Datastores.
 * @param  {Function} cb Executed upon completion.
 */
MerlinMongo.prototype.close = function(cb) {
  var self = this;

  // validate
  guard('cb', cb, [ 'function', 'undefined' ]);

  // defaults
  cb = cb || function() {};

  // validate
  if(typeof cb != 'function') { throw new Error('cb must be a function'); }

  self.collections = {};
  self.db.close(cb);
};

/**
 * Index a field within a collection.
 * @param  {String}   collectionName Collection name.
 * @param  {String}   path           Index field path.
 * @param  {Object}   opts           Indexing options.
 * @param  {Function} [cb]           Executed upon completion.
 */
MerlinMongo.prototype.index = function(collectionName, opts, path, cb) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, 'object');
  guard('opts.unique', opts.unique, [ 'boolean', 'undefined' ]);
  guard('opts.sparse', opts.sparse, [ 'boolean', 'undefined' ]);
  guard('path', path, 'string');
  guard('cb', cb, [ 'function', 'undefined' ]);

  // get the collection
  var collection = self.collections[collectionName];

  //TODO: Indexing
  collection.ensureIndex(path, {
    unique: opts.unique,
    sparse: opts.sparse
  }, cb);
};

/**
 * Count all records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Query}       query          Query.
 * @param  {Object}      opts           Find options.
 */
MerlinMongo.prototype.count = function(collectionName, opts, query) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, [ 'object', 'undefined' ]);
  guard('query', query, 'object');

  // get the collection and the query
  var collection = self.collections[collectionName];
  var mongoQuery = self._convertQuery(query);

  // count
  var cout = es.through();
  collection.count(mongoQuery, {
    skip: query.opts.offset || opts.offset,
    limit: query.opts.limit || opts.limit
  }, function(err, count) {
    if (err) { return cout.emit('error', err); }
    cout.push(count);
    cout.end();
  });

  return cout;
};

/**
 * Find all records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Find options.
 * @param  {Query}       query          Query.
 */
MerlinMongo.prototype.find = function(collectionName, opts, query) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, [ 'object', 'undefined' ]);
  guard('query', query, 'object');

  // get the collection, query and sort order
  var collection = self.collections[collectionName];
  var mongoQuery = self._convertQuery(query);
  var mongoSort = self._convertSort(query.opts.sort);

  // find the docs
  var rout = es.through();
  collection.find(mongoQuery, {
    skip: query.opts.offset || opts.offset,
    limit: query.opts.limit || opts.limit
  }, function(err, cursor) {
    if(err) { return rout.emit('error', err); }

    // grab each record
    cursor.each(function(err, record) {
      if(err) { return rout.emit('error', err); }
      if(record) { rout.push(record); }
      else { rout.end(); }
    });
  });

  return rout;
};

/**
 * Insert an array of records.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Insert options.
 * @param  {ModelStream} rin            Records input stream.
 * @param  {ModelStream} rout           Records output stream.
 */
MerlinMongo.prototype.insert = function(collectionName, opts) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, [ 'object', 'undefined' ]);

  // get the collection, and sort order.
  var collection = self.collections[collectionName];
  var mongoSort = self._convertSort(opts.sort);

  // insert the records
  var duplex = es.through();
  duplex.pipe(es.map(function(record, cb) {
    collection.insert(record, {}, function(err, record) {
      if (err) { return cb(err); }
      cb(null, record[0]);
    });
  })).pipe(duplex);

  return duplex;
};

/**
 * Insert an array of records.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Update options.
 * @param  {Query}       query          Query.
 * @param  {Delta}       delta          Delta.
 * @param  {CountStream} cout           Count stream.
 */
MerlinMongo.prototype.update = function(collectionName, opts, query, delta) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, [ 'object', 'undefined' ]);
  guard('query', query, 'object');
  guard('delta', delta, 'object');

  // get the collection and the query
  var collection = self.collections[collectionName];
  var mongoDelta = self._convertDelta(delta);
  var mongoQuery = self._convertQuery(query);

  // update the records
  var cout = es.through();
  collection.update(mongoQuery, mongoDelta, { multi: !opts.single }, function(err, count) {
    if(err) { return cout.emit('error', err); }
    cout.push(count);
    cout.end();
  });

  return cout;
};

/**
 * Remove records matching a query.
 * @param  {String}      collectionName Collection name.
 * @param  {Object}      opts           Remove options.
 * @param  {Query}       query          Query.
 */
MerlinMongo.prototype.remove = function(collectionName, opts, query) {
  var self = this;

  // validate
  guard('collectionName', collectionName, 'string');
  guard('opts', opts, [ 'object', 'undefined' ]);
  guard('query', query, 'object');

  // get the collection and the query
  var collection = self.collections[collectionName];
  var mongoQuery = self._convertQuery(query);

  // remove the records
  var cout = es.through();
  collection.remove(mongoQuery, {}, function(err, count) {
    if(err) { return cout.emit('error', err); }
    cout.push(count);
    cout.end();
  });

  return cout;
};

/**
 * Convert the Merlin Query to a NeDB Query object.
 * @private
 * @param  {Query}  query Merlin Query instance.
 * @return {Object}       NeDB query object.
 */
MerlinMongo.prototype._convertQuery = function(query) {

  // validate
  if(
    query === null || typeof query != 'object' ||
    query.query === null || typeof query.query != 'object' ||
    query.opts === null || typeof query.opts != 'object'
  ) { throw new Error('query must be an instance of Query'); }

  // build the new query
  return (function rec(vq) {
    var nq = {};
    var vqProps = Object.keys(vq);
    for(var i = 0; i < vqProps.length; i += 1) {
      var vqProp = vqProps[i];
      var vqVal = vq[vqProp];

      // if the value is an object then check to
      // see if its an operator object or a
      // regex. If it is then preform any
      // convertion that may be needed.
      if(vqVal !== null && typeof vqVal == 'object') {

        // check to see if this is an operator
        // object.
        for(var oProp in vqVal) { break; }
        if(oProp && oProp.charAt(0) == '$') {
          var nqVal = {};
          for(oProp in vqVal) {

            // $notIn => $nin
            if(oProp == '$notIn') { nqVal.$nin = vqVal[oProp]; }

            // $not => $ne
            else if(oProp == '$not') { nqVal.$ne = vqVal[oProp]; }

            // straight copy
            else { nqVal[oProp] = vqVal[oProp]; }
          }
          nq[vqProp] = nqVal;
        }

        // recurse
        else if(vqVal.constructor == Object) {
          nq[vqProp] = rec(vq[vqProp]);
        }

        // regex
        else if(vqVal.constructor == RegExp) {
          nq[vqProp] = { $regex: vqVal };
        }

        // other
        else {
          nq[vqProp] = vqVal;
        }
      }

      // copy non object properties.
      else {
        nq[vqProp] = vqVal;
      }
    }
    return nq;
  })(query.query);
};

/**
 * Convert the Merlin Delta to a NeDB Delta object.
 * @private
 * @param  {Query}  delta Merlin Delta instance.
 * @return {Object}       NeDB delta object.
 */
MerlinMongo.prototype._convertDelta = function(delta) {

  // build the new delta
  var vd = delta.diff;
  var md = {};
  for(var opt in vd) {

    // unset
    if(opt == '$unset') {
      md.$unset = {};
      for(var i = 0; i < vd.$unset.length; i += 1) {
        md.$unset[vd.$unset[i]] = true;
      }
    }

    // push
    else if(opt == '$push') {
      md.$push = {};
      for(var fieldPath in vd.$push) {
        md.$push[fieldPath] = { $each: vd.$push[fieldPath] };
      }
    }

    // pull
    else if(opt == '$pull') {
      md.$pullAll = {};
      for(var fieldPath in vd.$pull) {
        md.$pullAll[fieldPath] = vd.$pull[fieldPath];
      }
    }

    // everything else
    else {
      md[opt] = vd[opt];
    }
  }
  return md;
};



MerlinMongo.prototype._convertSort = function(sort) {
  var ms = null;
  if(sort !== null && typeof sort == 'object') {
    for(var i = 0; i < sort.length; i += 1) {
      for(var fieldPath in sort[i]) {
        if(sort[i].hasOwnProperty(fieldPath)) {
          if(!ms) { ms = []; }
          var sd = sort[i][fieldPath] === 'asc' && 1 || -1;
          ms.push([fieldPath, sd]);
        }
      }
    }
  }
  return ms;
};


module.exports = MerlinMongo;
