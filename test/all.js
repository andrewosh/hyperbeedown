const test = require('tape')
const hyperdown = require('..')

const suites = [
  require('abstract-leveldown/abstract/get-test'),
  require('abstract-leveldown/abstract/put-test'),
  require('abstract-leveldown/abstract/del-test'),
  require('abstract-leveldown/abstract/iterator-test'),
  require('abstract-leveldown/abstract/iterator-range-test')
]

suites.forEach(suite => {
  suite.all(hyperdown, test, null, {
    reduce: (a, b) => {
      if (!a) return b
      return a
    },
    map: ({ key, value }) => {
      return { key, value }
    },
    lex: true
  })
})


