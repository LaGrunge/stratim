// Copyright 2026 LaGrunge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { createPinia, setActivePinia } from 'pinia';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { Pipeline, Repo } from '~/lib/api/types';
import { usePipelineStore } from '~/store/pipelines';

interface EventData {
  pipeline?: Pipeline;
  repo?: Repo;
}

class FakeEventSource extends EventTarget {
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSED = 2;
  readyState = this.CONNECTING;
  closed = false;

  constructor(readonly callback: (data: EventData) => void) {
    super();
  }

  open() {
    this.readyState = this.OPEN;
    this.dispatchEvent(new Event('open'));
  }

  fail(readyState: number) {
    this.readyState = readyState;
    this.dispatchEvent(new Event('error'));
  }

  close() {
    this.closed = true;
    this.readyState = this.CLOSED;
  }
}

const streams: FakeEventSource[] = [];
const getPipeline = vi.fn();
const getPipelineList = vi.fn();
const getPipelineFeed = vi.fn();

vi.mock('~/compositions/useApiClient', () => ({
  default: () => ({
    // eslint-disable-next-line promise/prefer-await-to-callbacks
    on: (callback: (data: EventData) => void) => {
      const stream = new FakeEventSource(callback);
      streams.push(stream);
      return stream;
    },
    getPipeline,
    getPipelineList,
    getPipelineFeed,
  }),
}));

const repo = { id: 1 } as Repo;

function pipeline(number: number, status: Pipeline['status']): Pipeline {
  return { id: number, number, status } as Pipeline;
}

async function startEvents() {
  // useEvents only ever starts once per module instance
  vi.resetModules();
  const { default: useEvents } = await import('./useEvents');
  useEvents();
}

const last = () => streams[streams.length - 1];

// expectReconnectAfter checks the next stream opens exactly ms after now.
async function expectReconnectAfter(ms: number) {
  const before = streams.length;
  await vi.advanceTimersByTimeAsync(ms - 1);
  expect(streams).toHaveLength(before);
  await vi.advanceTimersByTimeAsync(1);
  expect(streams).toHaveLength(before + 1);
}

describe('useEvents', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // no jitter: each wait is the base delay
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    setActivePinia(createPinia());
    streams.length = 0;
    getPipeline.mockReset();
    getPipelineList.mockReset();
    getPipelineFeed.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('applies pipeline updates in the order they arrive', async () => {
    await startEvents();
    const store = usePipelineStore();

    streams[0].callback({ repo, pipeline: pipeline(5, 'running') });
    streams[0].callback({ repo, pipeline: pipeline(5, 'success') });

    expect(store.pipelines.get(1)?.get(5)?.status).toBe('success');
  });

  it('backs off until a stream stayed open for a while', async () => {
    await startEvents();

    streams[0].fail(streams[0].CLOSED);
    expect(streams[0].closed).toBe(true);
    await expectReconnectAfter(1000);

    last().fail(last().CLOSED);
    await expectReconnectAfter(2000);

    // opening alone does not reset the backoff
    last().open();
    last().fail(last().CLOSED);
    await expectReconnectAfter(4000);

    // staying open does
    last().open();
    await vi.advanceTimersByTimeAsync(10_000);
    last().fail(last().CLOSED);
    await expectReconnectAfter(1000);

    // and so does a message
    last().fail(last().CLOSED);
    await expectReconnectAfter(2000);
    last().open();
    last().callback({});
    last().fail(last().CLOSED);
    await expectReconnectAfter(1000);
  });

  it('jitters the delay and caps it at 30s', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.999);
    await startEvents();

    for (let i = 0; i < 8; i++) {
      last().fail(last().CLOSED);
      await vi.advanceTimersByTimeAsync(30_000);
    }
    last().fail(last().CLOSED);
    await expectReconnectAfter(30_000);

    vi.spyOn(Math, 'random').mockReturnValue(0);
    last().fail(last().CLOSED);
    await expectReconnectAfter(15_000);
  });

  it('leaves reconnecting to the browser while it retries', async () => {
    await startEvents();
    streams[0].open();
    streams[0].fail(streams[0].CONNECTING);

    await vi.advanceTimersByTimeAsync(60_000);
    expect(streams).toHaveLength(1);
    expect(streams[0].closed).toBe(false);
  });

  it('reloads the active pipelines once the stream is back', async () => {
    await startEvents();
    const store = usePipelineStore();
    store.setPipeline(1, pipeline(5, 'running'));
    store.setPipeline(1, pipeline(4, 'success'));
    getPipeline.mockResolvedValue(pipeline(5, 'success'));

    streams[0].open();
    await vi.runAllTimersAsync();
    expect(getPipeline).not.toHaveBeenCalled();

    // the browser reconnects by itself after a while
    streams[0].fail(streams[0].CONNECTING);
    await vi.advanceTimersByTimeAsync(3000);
    streams[0].open();
    await vi.advanceTimersByTimeAsync(1000);

    expect(getPipeline).toHaveBeenCalledTimes(1);
    expect(getPipeline).toHaveBeenCalledWith(1, 5);
    expect(store.pipelines.get(1)?.get(5)?.status).toBe('success');
  });

  it('does not reload after a short gap', async () => {
    await startEvents();
    const store = usePipelineStore();
    store.setPipeline(1, pipeline(5, 'running'));

    streams[0].open();
    streams[0].fail(streams[0].CONNECTING);
    await vi.advanceTimersByTimeAsync(500);
    streams[0].open();
    await vi.advanceTimersByTimeAsync(5000);
    expect(getPipeline).not.toHaveBeenCalled();
  });

  it('reloads when the first stream closed before it ever opened', async () => {
    await startEvents();
    const store = usePipelineStore();
    store.setPipeline(1, pipeline(5, 'running'));
    getPipeline.mockResolvedValue(pipeline(5, 'success'));

    await vi.advanceTimersByTimeAsync(3000);
    streams[0].fail(streams[0].CLOSED);
    await expectReconnectAfter(1000);
    last().open();
    await vi.advanceTimersByTimeAsync(1000);

    expect(getPipeline).toHaveBeenCalledWith(1, 5);
    expect(store.pipelines.get(1)?.get(5)?.status).toBe('success');
  });

  it('picks up an approved pipeline and pipelines created during the gap', async () => {
    await startEvents();
    const store = usePipelineStore();
    getPipelineList.mockResolvedValueOnce([pipeline(5, 'blocked')]);
    await store.loadRepoPipelines(1);
    getPipelineFeed.mockResolvedValue([]);

    getPipeline.mockResolvedValue(pipeline(5, 'running'));
    getPipelineList.mockResolvedValue([pipeline(6, 'pending'), pipeline(5, 'running')]);

    streams[0].open();
    streams[0].fail(streams[0].CONNECTING);
    await vi.advanceTimersByTimeAsync(5000);
    streams[0].open();
    await vi.advanceTimersByTimeAsync(1000);

    expect(getPipeline).toHaveBeenCalledWith(1, 5);
    expect(getPipelineList).toHaveBeenLastCalledWith(1, { page: 1, perPage: 50 });
    expect(store.pipelines.get(1)?.get(5)?.status).toBe('running');
    expect(store.pipelines.get(1)?.get(6)?.status).toBe('pending');
    expect(getPipelineFeed).not.toHaveBeenCalled();
  });

  it('reloads at most four pipelines at once', async () => {
    await startEvents();
    const store = usePipelineStore();
    for (let n = 1; n <= 10; n++) {
      store.setPipeline(1, pipeline(n, 'running'));
    }
    let inFlight = 0;
    let maxInFlight = 0;
    getPipeline.mockImplementation(async (_repo: number, n: number) => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 10));
      inFlight--;
      return pipeline(n, 'success');
    });

    streams[0].open();
    streams[0].fail(streams[0].CONNECTING);
    await vi.advanceTimersByTimeAsync(3000);
    streams[0].open();
    await vi.advanceTimersByTimeAsync(2000);

    expect(getPipeline).toHaveBeenCalledTimes(10);
    expect(maxInFlight).toBe(4);
  });
});
