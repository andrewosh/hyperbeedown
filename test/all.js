const p = require('path')
const test = require('tape')
const hyperdb = require('hyperdb')
const corestore = require('corestore')
const rimraf = require('rimraf')

const TEST_DIR = p.join(__dirname, 'test-storage')

const hyperdown = require('..')

const suites = [
  require('abstract-leveldown/abstract/get-test'),
  require('abstract-leveldown/abstract/put-test'),
  require('abstract-leveldown/abstract/del-test'),
  require('abstract-leveldown/abstract/put-get-del-test'),
  require('abstract-leveldown/abstract/batch-test'),
  require('abstract-leveldown/abstract/iterator-test'),
  require('abstract-leveldown/abstract/iterator-range-test'),
  require('abstract-leveldown/abstract/chained-batch-test')
]

var store = corestore(TEST_DIR, {
  network: {
    disable: true
  }
})

var coreFactory = (key, opts) => {
  return store.get(key, opts)
}

var dbFactory = (key, opts) => {
  opts.lex = true
  return hyperdb(coreFactory, key, opts)
}

store.ready().then(() => {
  suites.forEach(suite => {
    suite.all(hyperdown, test, null, {
      reduce: (a, b) => {
        if (!a) return b
        return a
      },
      map: ({ key, value }) => {
        return { key, value }
      },
      factory: dbFactory
    })
  })
  rimraf.sync(TEST_DIR)
})
