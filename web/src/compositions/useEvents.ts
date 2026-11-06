import { usePipelineStore } from '~/store/pipelines';
import { useRepoStore } from '~/store/repos';

import useApiClient from './useApiClient';

const apiClient = useApiClient();
let initialized = false;

// Base delay before opening a new event stream after the browser gave up on
// one, doubled on every failed attempt up to the maximum. Each wait is
// jittered, so that clients cut off together do not come back together.
const minReconnectDelay = 1000;
const maxReconnectDelay = 30_000;
// A stream counts as healthy (and the backoff starts over) once it got a
// message or stayed open this long.
const healthyAfter = 10_000;
// A disruption shorter than this is not worth reloading for.
const minGapToReload = 2000;
// Reloads after a reconnect are spread over up to this long.
const maxReloadDelay = 1000;

export default () => {
  if (initialized) {
    return;
  }
  const repoStore = useRepoStore();
  const pipelineStore = usePipelineStore();

  initialized = true;

  let reconnectDelay = minReconnectDelay;
  // When the UI was last known to be in sync, set while the stream is down.
  let downSince: number | undefined;

  const reloadAfterGap = () => {
    if (downSince === undefined) {
      return;
    }
    const gap = Date.now() - downSince;
    downSince = undefined;
    // Events published while the stream was down are not replayed: fetch
    // what may have changed meanwhile.
    if (gap >= minGapToReload) {
      setTimeout(() => void pipelineStore.reloadAfterReconnect(), Math.random() * maxReloadDelay);
    }
  };

  const connect = () => {
    let healthyTimer: ReturnType<typeof setTimeout> | undefined;
    const markHealthy = () => {
      clearTimeout(healthyTimer);
      reconnectDelay = minReconnectDelay;
    };

    const events = apiClient.on((data) => {
      markHealthy();

      // contains repo update
      if (!data.repo) {
        return;
      }
      const { repo } = data;
      repoStore.setRepo(repo);

      // contains pipeline update
      if (!data.pipeline) {
        return;
      }
      const { pipeline } = data;
      pipelineStore.setPipeline(repo.id, pipeline);
    });

    events.addEventListener('open', () => {
      clearTimeout(healthyTimer);
      healthyTimer = setTimeout(markHealthy, healthyAfter);
      reloadAfterGap();
    });

    events.addEventListener('error', () => {
      clearTimeout(healthyTimer);
      downSince ??= Date.now();
      // On a network error the browser reconnects by itself. An HTTP error
      // response (e.g. a proxy answering while the server restarts) closes
      // the stream for good, so open a new one.
      if (events.readyState !== events.CLOSED) {
        return;
      }
      events.close();
      const wait = Math.min(maxReconnectDelay, reconnectDelay * (0.5 + Math.random()));
      reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
      setTimeout(connect, wait);
    });
  };

  // Until the first stream opens, the UI may miss what happens after the
  // page loaded its data.
  downSince = Date.now();
  connect();
};
