'use strict'

const path = require('path')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const CID = require('cids')
const cat = require('pull-cat')
const cleanHash = require('./clean-multihash')

// Logic to export a unixfs directory.
module.exports = shardedDirExporter

function shardedDirExporter (node, name, ipldResolver, resolve, parent) {
  let dir
  if (!parent || parent.path !== name) {
    dir = [{
      path: name,
      hash: cleanHash(node.multihash)
    }]
  }

  return cat([
    pull.values(dir),
    pull(
      pull.values(node.links),
      pull.map((link) => ({
        name: link.name,
        path: path.join(name, link.name),
        hash: link.multihash
      })),
      paramap((item, cb) => ipldResolver.get(new CID(item.hash), (err, n) => {
        if (err) {
          return cb(err)
        }

        cb(null, resolve(n, item.path, ipldResolver, (dir && dir[0]) || parent))
      })),
      pull.flatten()
    )
  ])
}
