import test from 'node:test';
import assert from 'node:assert/strict';

import {
  LifecycleController,
  LifecycleError,
  LIFECYCLE_STATES
} from '../src/index.js';

test('lifecycle starts idle', () => {
  const lifecycle =
    new LifecycleController();

  assert.equal(
    lifecycle.state,
    'idle'
  );

  assert.deepEqual(
    LIFECYCLE_STATES,
    [
      'idle',
      'running',
      'stopping',
      'stopped'
    ]
  );
});

test('lifecycle start creates an AbortSignal', () => {
  const lifecycle =
    new LifecycleController();

  const signal =
    lifecycle.start();

  assert.equal(
    lifecycle.state,
    'running'
  );

  assert.ok(
    signal instanceof AbortSignal
  );

  assert.equal(
    lifecycle.signal,
    signal
  );
});

test('lifecycle rejects duplicate start', () => {
  const lifecycle =
    new LifecycleController();

  lifecycle.start();

  assert.throws(
    () => lifecycle.start(),
    LifecycleError
  );

  lifecycle.stop();
});

test('lifecycle stop aborts its signal', () => {
  const lifecycle =
    new LifecycleController();

  const signal =
    lifecycle.start();

  assert.equal(
    signal.aborted,
    false
  );

  assert.equal(
    lifecycle.stop(),
    true
  );

  assert.equal(
    signal.aborted,
    true
  );

  assert.equal(
    lifecycle.state,
    'stopped'
  );
});

test('stopping an idle lifecycle is harmless', () => {
  const lifecycle =
    new LifecycleController();

  assert.equal(
    lifecycle.stop(),
    false
  );

  assert.equal(
    lifecycle.state,
    'idle'
  );
});

test('reset returns stopped lifecycle to idle', () => {
  const lifecycle =
    new LifecycleController();

  lifecycle.start();
  lifecycle.stop();

  lifecycle.reset();

  assert.equal(
    lifecycle.state,
    'idle'
  );

  assert.equal(
    lifecycle.signal,
    null
  );
});

test('reset while running throws', () => {
  const lifecycle =
    new LifecycleController();

  lifecycle.start();

  assert.throws(
    () => lifecycle.reset(),
    LifecycleError
  );

  lifecycle.stop();
});

test('reset is chainable', () => {
  const lifecycle =
    new LifecycleController();

  lifecycle.start();
  lifecycle.stop();

  assert.equal(
    lifecycle.reset(),
    lifecycle
  );
});
