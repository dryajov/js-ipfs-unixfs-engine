'use strict'

const asyncEachSeries = require('async/eachSeries')
const waterfall = require('async/waterfall')
const CID = require('cids')
const dagPB = require('ipld-dag-pb')
const UnixFS = require('ipfs-unixfs')
const DAGLink = dagPB.DAGLink
const DAGNode = dagPB.DAGNode

class DirList {

  constructor (props) {
    this._children = {}
    Object.assign(this, props)
  }

  put (name, value, callback) {
    this.multihash = undefined
    this.size = undefined
    this._children[name] = value
    process.nextTick(callback)
  }

  get (name, callback) {
    process.nextTick(() => callback(null, this._children[name]))
  }

  childCount () {
    return Object.keys(this._children).length
  }

  onlyChild (callback) {
    process.nextTick(() => callback(null, this._children[Object.keys(this._children)[0]]))
  }

  eachChildSeries (iterator, callback) {
    asyncEachSeries(
      Object.keys(this._children),
      (key, callback) => {
        iterator(key, this._children[key], callback)
      },
      callback
    )
  }

  flush (path, ipldResolver, source, callback) {
    const keys = Object.keys(this._children)
    const dir = new UnixFS('directory')
    const links = keys
      .map((key) => {
        const child = this._children[key]
        return new DAGLink(key, child.size, child.multihash)
      })

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
          this.multihash = node.multihash
          this.size = node.size
          const pushable = {
            path: path,
            multihash: node.multihash,
            size: node.size
          }
          source.push(pushable)
          callback(null, node.multihash)
        }
      ],
      callback)
  }

}

module.exports = createDirList

function createDirList (props) {
  return new DirList(props)
}
