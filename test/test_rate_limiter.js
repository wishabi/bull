'use strict';

const expect = require('chai').expect;
const utils = require('./utils');
const redis = require('ioredis');
const _ = require('lodash');
const assert = require('assert');

describe('Rate limiter', () => {
  let queue;
  let client;

  beforeEach(() => {
    client = new redis();
    return client.flushdb().then(() => {
      queue = utils.buildQueue('test rate limiter', {
        limiter: {
          max: 1,
          duration: 1000
        }
      });
      return queue;
    });
  });

  afterEach(() => {
    return queue.close().then(() => {
      return client.quit();
    });
  });

  it('should throw exception if missing duration option', done => {
    try {
      utils.buildQueue('rate limiter fail', {
        limiter: {
          max: 5
        }
      });
      expect.fail('Should not allow missing `duration` option');
    } catch (err) {
      done();
    }
  });

  it('should throw exception if missing max option', done => {
    try {
      utils.buildQueue('rate limiter fail', {
        limiter: {
          duration: 5000
        }
      });
      expect.fail('Should not allow missing `max`option');
    } catch (err) {
      done();
    }
  });

  it.skip('should obey the rate limit', done => {
    const startTime = new Date().getTime();
    const numJobs = 4;

    queue.process(() => {
      return Promise.resolve();
    });

    for (let i = 0; i < numJobs; i++) {
      queue.add({});
    }

    queue.on(
      'completed',
      // after every job has been completed
      _.after(numJobs, () => {
        try {
          const timeDiff = new Date().getTime() - startTime;
          expect(timeDiff).to.be.above((numJobs - 1) * 1000);
          done();
        } catch (err) {
          done(err);
        }
      })
    );

    queue.on('failed', err => {
      done(err);
    });
  });

  it('should put rate limited jobs into waiting when bounceBack is true', async () => {
    const newQueue = utils.buildQueue('test rate limiter', {
      limiter: {
        max: 1,
        duration: 1000,
        bounceBack: true
      }
    });

    newQueue.on('failed', e => {
      assert.fail(e);
    });

    await Promise.all([
      newQueue.add({}),
      newQueue.add({}),
      newQueue.add({}),
      newQueue.add({})
    ]);

    await Promise.all([
      newQueue.getNextJob({}),
      newQueue.getNextJob({}),
      newQueue.getNextJob({}),
      newQueue.getNextJob({})
    ]);

    const completedCount = await newQueue.getCompletedCount();
    const failedCount = await newQueue.getFailedCount();
    const delayedCount = await newQueue.getDelayedCount();
    const activeCount = await newQueue.getActiveCount();
    const waitingCount = await newQueue.getWaitingCount();

    expect(completedCount).to.eq(0);
    expect(failedCount).to.eq(0);
    expect(delayedCount).to.eq(0);
    expect(activeCount).to.eq(1);
    expect(waitingCount).to.eq(3);
  });

  it('should put rate limited jobs into delayed when bounceBack is false', async () => {
    const newQueue = utils.buildQueue('test rate limiter', {
      limiter: {
        max: 1,
        duration: 1000,
        bounceBack: false
      }
    });

    newQueue.on('failed', e => {
      assert.fail(e);
    });

    await Promise.all([
      newQueue.add({}),
      newQueue.add({}),
      newQueue.add({}),
      newQueue.add({})
    ]);

    await Promise.all([
      newQueue.getNextJob({}),
      newQueue.getNextJob({}),
      newQueue.getNextJob({}),
      newQueue.getNextJob({})
    ]);

    const completedCount = await newQueue.getCompletedCount();
    const failedCount = await newQueue.getFailedCount();
    const delayedCount = await newQueue.getDelayedCount();
    const activeCount = await newQueue.getActiveCount();
    const waitingCount = await newQueue.getWaitingCount();

    expect(completedCount).to.eq(0);
    expect(failedCount).to.eq(0);
    expect(delayedCount).to.eq(3);
    expect(activeCount).to.eq(1);
    expect(waitingCount).to.eq(0);
  });

  it('should rate limit by grouping', async function() {
    this.timeout(20000);
    const numGroups = 4;
    const numJobs = 20;
    const startTime = Date.now();

    const rateLimitedQueue = utils.buildQueue('test rate limiter with group', {
      limiter: {
        max: 1,
        duration: 1000,
        groupKey: 'accountId'
      }
    });

    rateLimitedQueue.process(() => {
      return Promise.resolve();
    });

    const completed = {};

    const running = new Promise((resolve, reject) => {
      const afterJobs = _.after(numJobs, () => {
        try {
          const timeDiff = Date.now() - startTime;
          expect(timeDiff).to.be.gte(numGroups * 1000);
          expect(timeDiff).to.be.below((numGroups + 1) * 1500);

          for (const group in completed) {
            let prevTime = completed[group][0];
            for (let i = 1; i < completed[group].length; i++) {
              const diff = completed[group][i] - prevTime;
              expect(diff).to.be.below(2100);
              expect(diff).to.be.gte(900);
              prevTime = completed[group][i];
            }
          }
          resolve();
        } catch (err) {
          reject(err);
        }
      });

      rateLimitedQueue.on('completed', ({ id }) => {
        const group = _.last(id.split(':'));
        completed[group] = completed[group] || [];
        completed[group].push(Date.now());

        afterJobs();
      });

      rateLimitedQueue.on('failed', async err => {
        await queue.close();
        reject(err);
      });
    });

    for (let i = 0; i < numJobs; i++) {
      rateLimitedQueue.add({ accountId: i % numGroups });
    }

    await running;
    await rateLimitedQueue.close();
  });
});
