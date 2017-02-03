'use strict'

const traverse = require('pull-traverse')
const pull = require('pull-stream')
const CID = require('cids')
const isIPFS = require('is-ipfs')

const resolve = require('./resolve').resolve
const cleanMultihash = require('./clean-multihash')

module.exports = (hash, ipldResolver, options) => {
  if (!isIPFS.multihash(hash)) {
    return pull.error(new Error('not valid multihash'))
  }

  hash = cleanMultihash(hash)
  options = options || {}
  const parent = {
    path: hash,
    hash: hash
  }

  return pull(
    ipldResolver.getStream(new CID(hash)),
    pull.map((node) => {
      return resolve(node, hash, ipldResolver, parent)
    }),
    pull.flatten()
  )
}
