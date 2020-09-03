# HyperbeeDown
[![Build Status](https://travis-ci.org/andrewosh/hyperbeedown.svg?branch=master)](https://travis-ci.org/andrewosh/hyperbeedown)

A Leveldown for Hyperbee.

## Installation
`npm i hyperbeedown --save`

## Usage
```js
const core = hypercore(ram)
const tree = new Hyperbee(core)
await tree.ready()

const down = new HyperbeeDown(tree)
const db = levelup(down)

await db.put('hello', 'world')
console.log(await db.get('hello'))
```

## License
MIT
