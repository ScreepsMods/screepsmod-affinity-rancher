const request = require('request-promise')

module.exports = function engine (config) {
  config.engine.on('init', function (processType) {
    if (processType === 'main') {
      let queue = config.engine.driver.queue
      let users = queue.create('users', 'write')
      getMetadata()
        .then(meta => {
          let { scale } = meta.service
          let queues = []
          let ind = 0
          for (let i = 1; i <= scale; i++) {
            queues.push(queue.create(`users${i}`, 'read'))
          }
          users.whenAllDone = function () {
            return Promise.resolve(queues)
              .map(q => q.whenAllDone())
              .all()
          }
          function loop () {
            users.fetch()
              .then(user => {
                queues[ind].add(user)
                ind++
                ind %= scale
              })
              .finally(() => setTimeout(loop, 1))
          }
          loop()
        })
    }
    if (processType === 'runner') {
      let queue = config.engine.driver.queue
      let ocreate = queue.create
      queue.create = function (name, mode) {
        return getMetadata()
          .then(meta => {
            let serviceIndex = meta.container.service_index
            return ocreate(`${name}${serviceIndex}`, mode)
          })
      }
    }
  })
}

function getMetadata () {
  return request.get('http://rancher-metadata/latest/self', { json: true, headers: { accept: 'application/json' } })
}
