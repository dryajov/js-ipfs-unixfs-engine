'use strict'

const mapSeries = require('async/mapSeries')
const waterfall = require('async/waterfall')
const CID = require('cids')
const dagPB = require('ipld-dag-pb')
const UnixFS = require('ipfs-unixfs')
const DAGLink = dagPB.DAGLink
const DAGNode = dagPB.DAGNode
const multihashing = require('multihashing-async')

const Bucket = require('../hamt')

const hashFn = function (value, callback) {
  multihashing(value, 'murmur3', callback)
}
hashFn.code = 0x22 // TODO: get this from multihashing-async?

const defaultOptions = {
  hashFn: hashFn
}

class DirSharded {
  constructor (props, _options) {
    const options = Object.assign({}, defaultOptions, _options)
    this._options = options
    this._bucket = Bucket(options)
    Object.assign(this, props)
  }

  put (name, value, callback) {
    this._bucket.put(name, value, callback)
  }

  get (name, callback) {
    this._bucket.get(name, callback)
  }

  childCount () {
    return this._bucket.leafCount()
  }

  directChildrenCount () {
    return this._bucket.childrenCount()
  }

  onlyChild (callback) {
    this._bucket.onlyChild(callback)
  }

  eachChildSeries (iterator, callback) {
    this._bucket.eachLeafSeries(iterator, callback)
  }

  flush (path, ipldResolver, source, callback) {
    flush(this._options, this._bucket, path, ipldResolver, source, (err, node) => {
      if (err) {
        callback(err)
      } else {
        this.multihash = node.multihash
        this.size = node.size
      }
      callback(null, node)
    })
  }
}

module.exports = createDirSharded

function createDirSharded (props) {
  return new DirSharded(props)
}

function flush (options, bucket, path, ipldResolver, source, callback) {
  const children = bucket._children // TODO: intromission
  mapSeries(
    children.compactArray(),
    (child, cb) => {
      if (Bucket.isBucket(child)) {
        flush(options, child, path, ipldResolver, source, (err, node) => {
          if (err) {
            cb(err)
            return // early
          }
          cb(null, new DAGLink(null, node.size, node.multihash))
        })
      } else {
        const value = child.value
        cb(null, new DAGLink(child.key, value.size, value.multihash))
      }
    },
    (err, links) => {
      if (err) {
        callback(err)
      } else {
        haveLinks(links)
      }
    })

  function haveLinks (links) {
    const dir = new UnixFS('hamt-sharded-directory', new Buffer(children.bitField()))
    dir.fanout = bucket.tableSize()
    dir.hashType = options.hashFn.code
    waterfall(
      [
        (callback) => DAGNode.create(dir.marshal(), links, callback),
        (node, callback) => {
          ipldResolver.put(
            {
              node: node,
              cid: new CID(node.multihash)
            },
            (err) => callback(err, node))
        },
        (node, callback) => {
          const pushable = {
            path: path,
            multihash: node.multihash,
            size: node.size
          }
          // console.log('PUSHING', mh.toB58String(pushable.multihash))
          source.push(pushable)
          callback(null, node)
        }
      ],
      callback)
  }
}
