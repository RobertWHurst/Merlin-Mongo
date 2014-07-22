

// lib
var MerlinMongo = require('./lib/merlin-mongo');


/**
 * Merlin Mongo factory.
 * @param  {Object}   opts MerlinMongo opts.
 * @return {Function}      MerlinMongo inner factory.
 */
function merlinMongoFactory(opts) {

  // defaults
  opts = opts || {};

  // validate
  if(opts === null || typeof opts != 'object') {
    throw new Error('opts must be an object');
  }

  /**
   * Merlin Mongo inner factory.
   * @param  {Object}   merlin Merlin instance.
   * @return {Function}         MerlinMongo instance.
   */
  return function(merlin) {

    // validate
    if(merlin === null || typeof merlin != 'object') {
      throw new Error('merlin must be an object');
    }

    // return new merlin nedb instance.
    return new MerlinMongo(merlin, opts);
  };
}


exports = module.exports = merlinMongoFactory;
exports.MerlinMongo = MerlinMongo;
