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
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { Pipeline } from '~/lib/api/types';

import { usePipelineStore } from './pipelines';

const getPipeline = vi.fn();
const getPipelineList = vi.fn();

vi.mock('~/compositions/useApiClient', () => ({
  default: () => ({ getPipeline, getPipelineList }),
}));

describe('pipeline store', () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    getPipeline.mockReset();
    getPipelineList.mockReset();
  });

  it('keeps what only the full pipeline says when a list item comes in', async () => {
    const store = usePipelineStore();
    getPipeline.mockResolvedValue({ id: 5, number: 5, status: 'failure', can_restart_failed: true });
    await store.loadPipeline(1, 5);

    // a list item comes without workflows, so without can_restart_failed
    getPipelineList.mockResolvedValue([{ id: 5, number: 5, status: 'failure' } as Pipeline]);
    await store.loadRepoPipelines(1);
    await store.reloadAfterReconnect();

    expect(store.pipelines.get(1)?.get(5)?.can_restart_failed).toBe(true);
  });
});
