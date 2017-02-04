'use strict'

const path = require('path')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const CID = require('cids')

// Logic to export a unixfs directory.
module.exports = shardedDirExporter

function shardedDirExporter (node, name, ipldResolver, resolve) {
  return pull(
    pull.values(node.links),
    pull.map((link) => ({
      path: path.join(name, link.name),
      hash: link.multihash
    })),
    paramap((item, cb) => ipldResolver.get(new CID(item.hash), (err, n) => {
      if (err) {
        return cb(err)
      }

      const dir = {
        path: item.path,
        size: item.size
      }
      cb(null, resolve(n, item.path, ipldResolver, dir))
    })),
    pull.flatten()
  )
}
