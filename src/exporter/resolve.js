'use strict'

const UnixFS = require('ipfs-unixfs')
const pull = require('pull-stream')
const cat = require('pull-cat')

const resolvers = {
  directory: require('./dir-flat'),
  'hamt-sharded-directory': require('./dir-hamt-sharded'),
  file: require('./file')
}

module.exports = Object.assign({
  resolve: resolve,
  typeOf: typeOf
}, resolvers)

function resolve (node, name, ipldResolver, parent) {
  const type = typeOf(node)
  const resolver = resolvers[type]
  if (!resolver) {
    return pull.error(new Error('Unkown node type ' + type))
  }
  let stream = resolver(node, name, ipldResolver, resolve)
  if (parent && type !== 'file') {
    stream = cat([pull.values([parent]), stream])
  }
  return stream
}

function typeOf (node) {
  const data = UnixFS.unmarshal(node.data)
  return data.type
}
