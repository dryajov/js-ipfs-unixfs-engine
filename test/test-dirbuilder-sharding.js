/* eslint-env mocha */
'use strict'

const importer = require('./../src').importer
const exporter = require('./../src').exporter

const mh = require('multihashes')
const expect = require('chai').expect
const BlockService = require('ipfs-block-service')
const IPLDResolver = require('ipld-resolver')
const pull = require('pull-stream')

module.exports = (repo) => {
  describe('dirbuilder sharding', () => {
    let nonShardedHash, shardedHash
    let ipldResolver

    before(() => {
      const bs = new BlockService(repo)
      ipldResolver = new IPLDResolver(bs)
    })

    it('yields a non-sharded dir', (done) => {
      const options = {
        shardSplitThreshold: Infinity // never shard
      }

      pull(
        pull.values([
          {
            path: 'a/b',
            content: pull.values([new Buffer('i have the best bytes')])
          }
        ]),
        importer(ipldResolver, options),
        pull.collect((err, nodes) => {
          expect(err).to.not.exist
          expect(nodes.length).to.be.eql(2)
          expect(nodes[0].path).to.be.eql('a/b')
          expect(nodes[1].path).to.be.eql('a')
          nonShardedHash = nodes[1].multihash
          expect(nonShardedHash).to.exist
          done()
        })
      )
    })

    it('yields a sharded dir', (done) => {
      const options = {
        shardSplitThreshold: 0 // always shard
      }

      pull(
        pull.values([
          {
            path: 'a/b',
            content: pull.values([new Buffer('i have the best bytes')])
          }
        ]),
        importer(ipldResolver, options),
        pull.collect((err, nodes) => {
          expect(err).to.not.exist
          expect(nodes.length).to.be.eql(2)
          expect(nodes[0].path).to.be.eql('a/b')
          expect(nodes[1].path).to.be.eql('a')
          shardedHash = nodes[1].multihash
          // hashes are different
          expect(shardedHash).to.not.equal(nonShardedHash)
          done()
        })
      )
    })

    it('exporting unsharded hash results in the correct files', (done) => {
      pull(
        exporter(nonShardedHash, ipldResolver),
        pull.collect((err, nodes) => {
          expect(err).to.not.exist
          expect(nodes.length).to.be.eql(2)
          const expectedHash = mh.toB58String(nonShardedHash)
          expect(nodes[0].path).to.be.eql(expectedHash)
          expect(nodes[0].hash).to.be.eql(expectedHash)
          expect(nodes[1].path).to.be.eql(expectedHash + '/b')
          expect(nodes[1].size).to.be.eql(21)
          pull(
            nodes[1].content,
            pull.collect((err, content) => {
              expect(err).to.not.exist
              expect(content.length).to.be.eql(1)
              expect(content[0].toString()).to.be.eql('i have the best bytes')
              done()
            })
          )
        })
      )
    })

    it('exporting sharded hash results in the correct files', (done) => {
      pull(
        exporter(shardedHash, ipldResolver),
        pull.collect((err, nodes) => {
          expect(err).to.not.exist
          expect(nodes.length).to.be.eql(2)
          const expectedHash = mh.toB58String(shardedHash)
          expect(nodes[0].path).to.be.eql(expectedHash)
          expect(nodes[0].hash).to.be.eql(expectedHash)
          expect(nodes[1].path).to.be.eql(expectedHash + '/b')
          expect(nodes[1].size).to.be.eql(21)
          pull(
            nodes[1].content,
            pull.collect((err, content) => {
              expect(err).to.not.exist
              expect(content.length).to.be.eql(1)
              expect(content[0].toString()).to.be.eql('i have the best bytes')
              done()
            })
          )
        })
      )
    })
  })
}
