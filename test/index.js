
// modules
var test = require('tape');

// libs
var merlinMongo = require('../');

// constants
var VICEROY = {
  opts: {},
  models: {
    Test: { collectionName: 'tests' }
  }
};
var VICEROY_MONGO_OPTS = {
  databaseUrl: 'mongodb://localhost:27017/merlin-mongo'
};

test('merlinMongoFactory()', function(t) {
  t.throws(function() { merlinMongo(1); });
  t.throws(function() { merlinMongo('s'); });
  t.throws(function() { merlinMongo(true); });
  t.doesNotThrow(function() { merlinMongo(); });
  t.doesNotThrow(function() { merlinMongo(VICEROY_MONGO_OPTS); });

  t.equal(typeof merlinMongo(), 'function');

  t.end();
});

test('merlinMongoInnerFactory()', function(t) {
  var inner = merlinMongo(VICEROY_MONGO_OPTS);
  t.throws(function() { inner(); });
  t.throws(function() { inner(null); });
  t.throws(function() { inner(1); });
  t.throws(function() { inner(false); });
  t.throws(function() { inner('s'); });
  t.doesNotThrow(function() { inner(VICEROY); });

  var vMongo = inner(VICEROY);
  t.equal(typeof vMongo, 'object');

  t.end();
});
