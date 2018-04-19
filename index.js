const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const hyperdb = require('hyperdb')
const inherits = require('inherits')

const Status = {
  NEW: 'new',
  OPENING: 'opening',
  OPEN: 'open',
  CLOSING: 'closing',
  CLOSED: 'closed'
}

module.exports = HyperDown

function HyperDown (storage, key, opts) {
  if (!(this instanceof HyperDown)) return new HyperDown(storage, key, opts)

  opts.reduce = opts.reduce || defaultReduce
  this._db = hyperdb(storage, key, opts)
  this.status = Status.NEW

  AbstractLevelDOWN.call(this, location)
}
inherits(HyperDown, AbstractLevelDOWN)

HyperDown.prototype._open = function (opts, cb) {
  this.status = Status.OPENING
  this._db.ready(err => {
    if (err) return cb(err)
    this.status = Status.OPEN
    return cb()
  })
}

HyperDown.prototype._get = function (key, opts, cb) {
  return this._db.get(key, opts, cb)     
}

HyperDown.prototype._put = function (key, value, opts, cb) {
  return this._db.put(key, value, cb)
}

HyperDown.prototype._del = function (key, opts, cb) {
  return this._db.del(key, cb)
}

HyperDown.prototype._batch = function (array, opts, cb) {
  return this._db.batch(array, cb)
}

HyperDown.prototype._iterator = function (opts) {
  return this._db.iterator(opts)
}

HyperDown.prototype.status = function () {
  return this.status
}

function defaultReduce (nodes) {
  if (!nodes || !nodes.length) return null  
  return nodes[0]
}
