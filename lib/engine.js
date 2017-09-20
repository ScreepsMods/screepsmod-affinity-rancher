const request = require('request-promise')
const Promise = require('bluebird')

module.exports = function engine (config) {
  config.engine.on('init', function (processType) {
    if (processType === 'main') {
      let queue = config.engine.driver.queue
      let users = queue.create('users', 'write')
      let queues = []
      let { reset } = users
      config.engine.on('mainLoopStage', (stage, users) => {
        if (stage === 'addUsersToQueue') {
          let cnts = {}
          let ind = 0
          users.forEach(({ _id, rooms }, i) => {
            let ind = i % queues.length
            if (!rooms || rooms.length) {
              console.log(`${ind}/${queues.length}`, _id, rooms)
              queues[ind].add(_id)
            } else {
              console.error('Queues empty!')
            }
            cnts[ind] = cnts[ind] || 0
            cnts[ind]++
          })
          console.log('[affinity-rancher]', 'users queue counts', cnts)
          reset()
        }
      })
      users.whenAllDone = function () {
        return Promise.resolve()
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
