const test = require('tape')
const ram = require('random-access-memory')
const suite = require('abstract-leveldown/test')

const HyperDown = require('..')

suite({
  test,
  snapshots: false,
  seek: false,
  createIfMissing: false,
  errorIfExists: false,
  factory: () => {
    return new HyperDown(ram)
  }
})
