/* eslint-env mocha */
'use strict'

const importer = require('./../src').importer

const expect = require('chai').expect
const BlockService = require('ipfs-block-service')
const IPLDResolver = require('ipld-resolver')
const pull = require('pull-stream')

module.exports = (repo) => {
  describe('dirbuilder sharding', () => {
    let ipldResolver

    before(() => {
      const bs = new BlockService(repo)
      ipldResolver = new IPLDResolver(bs)
    })

    it('yields a sharded dir', (done) => {
      const options = {
        // shardSplitThreshold: 0 // always shard
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
          console.log(nodes.length)
          expect(nodes.length).to.be.eql(2)
          expect(nodes[0].path).to.be.eql('a/b')
          expect(nodes[1].path).to.be.eql('a')
          done()
        })
      )
    })
  })
}
