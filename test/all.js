const test = require('tape')
const ram = require('random-access-memory')
const hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const suite = require('abstract-leveldown/test')

const HyperbeeDown = require('..')

suite({
  test,
  seek: false,
  createIfMissing: false,
  errorIfExists: false,
  factory: () => {
    const tree = new Hyperbee(hypercore(ram), {
      keyEncoding: 'utf8'
    })
    return new HyperbeeDown(tree)
  }
})
