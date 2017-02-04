'use strict'

const eachSeries = require('async/eachSeries')
const eachOfSeries = require('async/eachOfSeries')
const waterfall = require('async/waterfall')
const createQueue = require('async/queue')
const writable = require('pull-write')
const pushable = require('pull-pushable')
const DirFlat = require('./dir-flat')
const flatToShard = require('./flat-to-shard')

module.exports = createTreeBuilder

const defaultOptions = {
  wrap: false,
  shardSplitThreshold: 1000
}

function createTreeBuilder (ipldResolver, _options) {
  const options = Object.assign({}, defaultOptions, _options)

  const queue = createQueue(consumeQueue, 1)

  // returned stream
  let stream = createStream()

  // root node
  let tree = DirFlat({
    path: '',
    root: true,
    dir: true,
    dirty: false,
    flat: true
  })

  return {
    flush: flushRoot,
    stream: getStream
  }

  function consumeQueue (action, callback) {
    const args = action.args.concat(function () {
      action.cb.apply(null, arguments)
      callback()
    })
    action.fn.apply(null, args)
  }

  function getStream () {
    return stream
  }

  function createStream () {
    const sink = writable(write, null, 1, ended)
    const source = pushable()

    return {
      sink: sink,
      source: source
    }

    function write (elems, callback) {
      eachSeries(
        elems,
        (elem, callback) => {
          queue.push({
            fn: addToTree,
            args: [elem],
            cb: (err) => {
              if (err) {
                callback(err)
              } else {
                source.push(elem)
                callback()
              }
            }
          })
        },
        callback
      )
    }

    function ended (err) {
      flush('', tree, (flushErr) => {
        source.end(flushErr || err)
      })
    }
  }

  // ---- Add to tree

  function addToTree (elem, callback) {
    const pathElems = elem.path.split('/').filter(notEmpty)
    let parent = tree
    const lastIndex = pathElems.length - 1

    let currentPath = ''
    eachOfSeries(pathElems, (pathElem, index, callback) => {
      if (currentPath) {
        currentPath += '/'
      }
      currentPath += pathElem

      const last = (index === lastIndex)
      parent.dirty = true
      parent.multihash = null
      parent.size = null

      if (last) {
        waterfall([
          (callback) => parent.put(pathElem, elem, callback),
          (callback) => flatToShard(null, parent, options.shardSplitThreshold, callback),
          (newRoot, callback) => {
            tree = newRoot
            callback()
          }
        ], callback)
      } else {
        parent.get(pathElem, (err, treeNode) => {
          if (err) {
            callback(err)
            return // early
          }
          let dir = treeNode
          if (!dir) {
            dir = DirFlat({
              dir: true,
              parent: parent,
              parentKey: pathElem,
              path: currentPath,
              dirty: true,
              flat: true
            })
          }
          const parentDir = parent
          parent = dir
          parentDir.put(pathElem, dir, callback)
        })
      }
    }, callback)
  }

  // ---- Flush

  function flushRoot (callback) {
    queue.push({
      fn: flush,
      args: ['', tree],
      cb: callback
    })
  }

  function flush (path, tree, callback) {
    if (tree.dir) {
      if (tree.root && tree.childCount() > 1 && !options.wrap) {
        callback(new Error('detected more than one root'))
        return // early
      }
      tree.eachChildSeries(
        (key, child, callback) => {
          flush(path ? (path + '/' + key) : key, child, callback)
        },
        (err) => {
          if (err) {
            callback(err)
            return // early
          }
          flushDir(path, tree, callback)
        })
    } else {
      // leaf node, nothing to do here
      process.nextTick(callback)
    }
  }

  function flushDir (path, tree, callback) {
    // don't create a wrapping node unless the user explicitely said so
    if (tree.root && !options.wrap) {
      tree.onlyChild((err, onlyChild) => {
        if (err) {
          callback(err)
          return // early
        }

        const multihash = onlyChild && onlyChild.multihash
        callback(null, multihash)
      })

      return // early
    }

    if (!tree.dirty) {
      callback(null, tree.multihash)
      return // early
    }

    // don't flush directory unless it's been modified

    tree.dirty = false
    tree.flush(path, ipldResolver, stream.source, (err, node) => {
      if (err) {
        callback(err)
      } else {
        callback(null, node.multihash)
      }
    })
  }
}

function notEmpty (str) {
  return Boolean(str)
}

