const request = require('request-promise')
const Promise = require('bluebird')

module.exports = function engine (config) {
  config.engine.on('init', function (processType) {
    if (processType === 'main') {
      let queue = config.engine.driver.queue
      let users = queue.create('users', 'write')
      let queues = []
      let ind = 0
      let cnts = []
      let { reset, whenAllDone } = users
      users.whenAllDone = function () {
        return Promise.resolve()
          .then(whenAllDone)
          .then(() => {
            console.log('[affinity-rancher]', 'whenAllDone counts', cnts)
            cnts = []
          })
          .then(() => queues)
          .map(q => q.whenAllDone())
          .all()
      }
      users.reset = function () {
        return Promise.resolve(queues)
          .map(q => q.reset())
          .all()
          .then(reset)
      }

      let loop = function loop () {
        users.fetch()
          .then(user => {
            if (queues.length) {
              console.log(ind, user)
              queues[ind].add(user)
            } else {
              console.error('Queues empty!')
            }
            cnts[ind] = cnts[ind] || 0
            cnts[ind]++
            ind++
            ind %= queues.length
            setTimeout(loop, 1)
            return users.markDone(user)
          })
      }
      loop()
      let updateScale = () => getRunnersScale()
        .then(scale => {
          queues = []
          for (let i = 1; i <= scale; i++) {
            queues.push(queue.create(`users${i}`, 'read'))
          }
          console.log('[affinity-rancher]', 'current scale is', scale)
        })
        .catch(err => console.error(err))
      updateScale()
      setInterval(updateScale, 5000)
    }
    if (processType === 'runner') {
      let queue = config.engine.driver.queue
      let ocreate = queue.create
      queue.create = function (name, mode) {
        return getOwnMetadata()
          .then(meta => {
            let serviceIndex = meta.container.service_index
            console.log('[affinity-rancher]', 'current index is', serviceIndex)
            return ocreate(`${name}${serviceIndex}`, mode)
          })
      }
    }
  })
}

function getOwnMetadata () {
  return request.get('http://rancher-metadata/latest/self', { json: true, headers: { accept: 'application/json' } })
}

function getRunnersScale () {
  return request.get('http://rancher-metadata/latest/self/stack/services/runners/scale', { json: true, headers: { accept: 'application/json' } })
}
