const test = require('tape')
const ram = require('random-access-memory')
const hypercore = require('hypercore')
const BTree = require('hyperb')
const suite = require('abstract-leveldown/test')

const HyperDown = require('..')

suite({
  test,
  seek: false,
  createIfMissing: false,
  errorIfExists: false,
  factory: () => {
    const tree = new BTree(hypercore(ram), {
      keyEncoding: 'utf8'
    })
    return new HyperDown(tree)
  }
})
