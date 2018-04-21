const {
  AbstractLevelDOWN,
  AbstractIterator
} = require('abstract-leveldown')
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

function HyperDown (storage) {
  if (!(this instanceof HyperDown)) return new HyperDown(storage)
  this.status = Status.NEW
  this.storage = storage

  // Set in _open.
  this._db = null

  AbstractLevelDOWN.call(this, storage)
}
inherits(HyperDown, AbstractLevelDOWN)

HyperDown.prototype._open = function (opts, cb) {
  if (typeof opts.reduce !== 'function') throw new Error('Reduce must be a function.')

  this.status = Status.OPENING
  this._db = hyperdb(this.storage, opts.key, opts)

  this._db.ready(err => {
    if (err) return cb(err)
    this.status = Status.OPEN
    return cb()
  })
}

HyperDown.prototype._get = function (key, opts, cb) {
  this._db.get(key, opts, function (err, node) {
    if (err) return cb(err)
    
    if (!node) {
      var err = new Error('NotFound')
      err.notFound = true
      return cb(err)
    }

    var value = node.value
    var key = node.key

    if (!opts.asBuffer) value = value.toString('utf-8')
    return cb(null, value)
  })
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
  if (opts.start) opts.gte = opts.start
  if (opts.end) opts.lte = end
  return new HyperIterator(this, opts)
}

HyperDown.prototype.status = function () {
  return this.status
}

function HyperIterator (db, opts) {
  var checkout = db._db.snapshot()
  this.ite = checkout.iterator(opts) 
  AbstractIterator.call(this, db)
}
inherits(HyperIterator, AbstractIterator)

HyperIterator.prototype._next = function (cb) {
  if (!cb) throw new Error('next() requires a callback argument')
  this.ite.next((err, node) => {
    console.log('ERR:', err, 'NODE:', node)
    if (err) return cb(err)
    if (!node) return cb()
    // TODO: Better buffer conversion for key?
    return cb(null, Buffer.from(node.key), node.value)
  })
}
