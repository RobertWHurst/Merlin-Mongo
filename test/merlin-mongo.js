
// modules
var test = require('tape');
var path = require('path');
var fs = require('fs');

// libs
var MerlinMongo = require('../').MerlinMongo;

// constants
var MERLIN = {
  opts: {},
  models: {
    Test: { collectionName: 'tests' }
  }
};
var MERLIN_MONGO_OPTS = {
  databaseUrl: 'mongodb://localhost:27017/merlin-mongo'
};

// globals
var merlinMongo = null;


test('MerlinMongo()', function(t) {

  t.throws(function() { new MerlinMongo(); });
  t.throws(function() { new MerlinMongo(null); });
  t.throws(function() { new MerlinMongo(1); });
  t.throws(function() { new MerlinMongo(false); });
  t.throws(function() { new MerlinMongo('s'); });
  t.throws(function() { new MerlinMongo({}); });
  t.throws(function() { new MerlinMongo({}, null); });
  t.throws(function() { new MerlinMongo({}, 1); });
  t.throws(function() { new MerlinMongo({}, false); });
  t.throws(function() { new MerlinMongo({}, 's'); });
  merlinMongo = new MerlinMongo(MERLIN, MERLIN_MONGO_OPTS);

  t.end();
});

test('merlinMongo{}', function(t) {

  t.equal(typeof merlinMongo.connect, 'function');
  t.equal(typeof merlinMongo.close, 'function');
  t.equal(typeof merlinMongo.index, 'function');
  t.equal(typeof merlinMongo.count, 'function');
  t.equal(typeof merlinMongo.find, 'function');
  t.equal(typeof merlinMongo.insert, 'function');
  t.equal(typeof merlinMongo.update, 'function');
  t.equal(typeof merlinMongo.remove, 'function');

  t.end();
});

test('merlinMongo.connect()', function(t) {
  t.throws(function() { merlinMongo.connect(1); });
  t.throws(function() { merlinMongo.connect('s'); });
  t.throws(function() { merlinMongo.connect({}); });
  merlinMongo.connect(function(err) {
    t.error(err);
    t.end();
  });
});

test('merlinMongo.index()', function(t) {
  t.doesNotThrow(function() { merlinMongo.index(); });
  t.doesNotThrow(function() { merlinMongo.index(1); });
  t.doesNotThrow(function() { merlinMongo.index('s'); });
  t.doesNotThrow(function() { merlinMongo.index({}); });
  t.doesNotThrow(function() { merlinMongo.index(false); });
  t.doesNotThrow(function() { merlinMongo.index('tests', 1, function(err) { t.ok(err); }); });
  t.doesNotThrow(function() { merlinMongo.index('tests', 's', function(err) { t.ok(err); }); });
  t.doesNotThrow(function() { merlinMongo.index('tests', false, function(err) { t.ok(err); }); });
  t.doesNotThrow(function() { merlinMongo.index('tests', {}, 1); });
  t.doesNotThrow(function() { merlinMongo.index('tests', {}, 's'); });
  merlinMongo.index('tests', {}, 's');
  t.doesNotThrow(function() { merlinMongo.index('tests', {}, false); });
  t.throws(function() { merlinMongo.index('tests', {}, 'name', 1); });
  t.throws(function() { merlinMongo.index('tests', {}, 'name', 's'); });


  merlinMongo.collections.tests.ensureIndex = function(fieldPath, opts, cb) {
    t.equal(fieldPath, 'name');
    t.equal(typeof opts, 'object');
    t.equal(opts.sparse, true);
    t.equal(opts.unique, true);
    cb(null);
  };

  merlinMongo.index('tests', { sparse: true, unique: true }, 'name', function(err) {
    t.error(err);
    t.end();
  });
});

test('merlinMongo.count()', function(t) {

  t.throws(function() { merlinMongo.count(); });
  t.throws(function() { merlinMongo.count(1); });
  t.throws(function() { merlinMongo.count('s'); });
  t.throws(function() { merlinMongo.count('tests', 1); });
  t.throws(function() { merlinMongo.count('tests', 's'); });
  t.throws(function() { merlinMongo.count('tests', false); });
  t.throws(function() { merlinMongo.count('tests', {}, 1); });
  t.throws(function() { merlinMongo.count('tests', {}, 's'); });

  merlinMongo.collections.tests.count = function(mongoQuery, opts, cb) {
    t.equal(typeof mongoQuery, 'object');
    t.equal(mongoQuery.name, 'test');
    cb(null, 10);
  };

  merlinMongo.count('tests', {}, {
    query: { name: 'test' },
    opts: {}
  }, {
    write: function(count) { t.equal(count, 10); },
    end: function() { t.end(); }
  });
});

test('merlinMongo.find()', function(t) {

  t.throws(function() { merlinMongo.find(); });
  t.throws(function() { merlinMongo.find(1); });
  t.throws(function() { merlinMongo.find('s'); });
  t.throws(function() { merlinMongo.find('tests', 1); });
  t.throws(function() { merlinMongo.find('tests', 's'); });
  t.throws(function() { merlinMongo.find('tests', false); });
  t.throws(function() { merlinMongo.find('tests', {}, 1); });
  t.throws(function() { merlinMongo.find('tests', {}, 's'); });
  t.throws(function() {
    merlinMongo.find('tests', {}, {
      query: {},
      opts: {}
    }, 1);
  });
  t.throws(function() {
    merlinMongo.find('tests', {}, {
      query: {},
      opts: {}
    }, 's');
  });

  merlinMongo.collections.tests.find = function(mongoQuery, opts, cb) {
    t.equal(typeof mongoQuery, 'object');
    t.equal(mongoQuery.name, 'test');
    cb(null, {
      each: function(cb) {
        cb(null, { name: 'test-a' });
        cb(null, { name: 'test-b' });
        cb(null, { name: 'test-c' });
        cb(null, null);
      }
    });
  };

  var recordNames = [
    'test-a',
    'test-b',
    'test-c'
  ];
  merlinMongo.find('tests', {}, {
    query: { name: 'test' },
    opts: {}
  }, {
    write: function(record) {
      var i = recordNames.indexOf(record.name);
      if(i !== -1) { recordNames.splice(i, 1); }
      t.notEqual(i, -1);
    },
    end: function() { t.end(); }
  });
});

test('merlinMongo.insert()', function(t) {

  t.throws(function() { merlinMongo.insert(); });
  t.throws(function() { merlinMongo.insert(1); });
  t.throws(function() { merlinMongo.insert('s'); });
  t.throws(function() { merlinMongo.insert('tests', 1); });
  t.throws(function() { merlinMongo.insert('tests', 's'); });
  t.throws(function() { merlinMongo.insert('tests', false); });
  t.throws(function() { merlinMongo.insert('tests', {}, 1); });
  t.throws(function() { merlinMongo.insert('tests', {}, 's'); });
  t.throws(function() {
    merlinMongo.insert('tests', {}, {
      each: function() {}
    }, 1);
  });
  t.throws(function() {
    merlinMongo.insert('tests', {}, {
      each: function() {}
    }, 's');
  });

  merlinMongo.collections.tests.insert = function(record, opts, cb) {
    t.equal(typeof record, 'object');
    cb(null, record);
  };

  var recordNames = [
    'test-a',
    'test-b',
    'test-c'
  ];
  merlinMongo.insert('tests', {}, {
    each: function(cb) {
      cb(null, { name: 'test-a' });
      cb(null, { name: 'test-b' });
      cb(null, { name: 'test-c' });
      cb(null, null);
    }
  }, {
    write: function(record) {
      var i = recordNames.indexOf(record.name);
      if(i !== -1) { recordNames.splice(i, 1); }
      t.notEqual(i, -1);
    },
    end: function() { t.end(); }
  });
});

test('merlinMongo.update()', function(t) {

  t.throws(function() { merlinMongo.update(); });
  t.throws(function() { merlinMongo.update(1); });
  t.throws(function() { merlinMongo.update('s'); });
  t.throws(function() { merlinMongo.update('tests', 1); });
  t.throws(function() { merlinMongo.update('tests', 's'); });
  t.throws(function() { merlinMongo.update('tests', false); });
  t.throws(function() { merlinMongo.update('tests', {}, 1); });
  t.throws(function() { merlinMongo.update('tests', {}, 's'); });
  t.throws(function() { merlinMongo.update('tests', {}, false); });
  t.throws(function() {
    merlinMongo.update('tests', {}, {
      query: {},
      opts: {}
    }, 1);
  });
  t.throws(function() {
    merlinMongo.update('tests', {}, {
      query: {},
      opts: {}
    }, 's');
  });
  t.throws(function() {
    merlinMongo.update('tests', {}, {
      query: {},
      opts: {}
    }, {}, 1);
  });
  t.throws(function() {
    merlinMongo.update('tests', {}, {
      query: {},
      opts: {}
    }, {}, 's');
  });

  merlinMongo.collections.tests.update = function(mongoQuery, mongoDelta, opts, cb) {
    t.equal(typeof mongoQuery, 'object');
    t.equal(mongoQuery.name, 'test');
    t.equal(typeof mongoDelta, 'object');
    t.equal(typeof mongoDelta.$set, 'object');
    t.equal(mongoDelta.$set.name, 'test-a');
    cb(null, 3);
  };

  merlinMongo.update('tests', {}, {
    query: { name: 'test' },
    opts: {}
  }, { diff: { $set: { name: 'test-a' }}}, {
    write: function(count) { t.equal(count, 3); },
    end: function() { t.end(); }
  });
});

test('merlinMongo.remove()', function(t) {

  t.throws(function() { merlinMongo.remove(); });
  t.throws(function() { merlinMongo.remove(1); });
  t.throws(function() { merlinMongo.remove('s'); });
  t.throws(function() { merlinMongo.remove('tests', 1); });
  t.throws(function() { merlinMongo.remove('tests', 's'); });
  t.throws(function() { merlinMongo.remove('tests', false); });
  t.throws(function() { merlinMongo.remove('tests', {}, 1); });
  t.throws(function() { merlinMongo.remove('tests', {}, 's'); });
  t.throws(function() { merlinMongo.remove('tests', {}, false); });
  t.throws(function() {
    merlinMongo.remove('tests', {}, {
      query: {},
      opts: {}
    }, 1);
  });
  t.throws(function() {
    merlinMongo.remove('tests', {}, {
      query: {},
      opts: {}
    }, 's');
  });

  merlinMongo.collections.tests.remove = function(mongoQuery, opts, cb) {
    t.equal(typeof mongoQuery, 'object');
    t.equal(mongoQuery.name, 'test');
    cb(null, 3);
  };

  merlinMongo.remove('tests', {}, {
    query: { name: 'test' },
    opts: {}
  }, {
    write: function(count) { t.equal(count, 3); },
    end: function() { t.end(); }
  });
});

test('merlinMongo.close()', function(t) {
  merlinMongo.close(function(err) {
    t.equal(merlinMongo.collections.tests, undefined);
    t.end();
  });
});
