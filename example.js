const HyperbeeDown = require('.')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const hypercore = require('hypercore')
const levelup = require('levelup')

start ()

async function start () {
  const core = hypercore(ram)
  const tree = new Hyperbee(core)
  const down = new HyperbeeDown(tree)
  const db = levelup(down)

  await db.put('hello', 'world')
  console.log(await db.get('hello'))
}
