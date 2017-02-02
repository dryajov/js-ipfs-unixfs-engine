'use strict'

const Shard = require('../hamt')

class DirSharded {

  constructor (props) {
    this._bucket = Shard()
    Object.assign(this, props)
  }

  put (name, value, callback) {
    this._bucket.put(name, value, callback)
  }

  asyncTransform (asyncMap, asyncReduce, callback) {
    this._bucket.asyncTransform(asyncMap, asyncReduce, callback)
  }
}

module.exports = DirSharded
