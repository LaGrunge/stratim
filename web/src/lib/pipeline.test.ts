import { describe, expect, it } from 'vitest';

import type { Pipeline } from '~/lib/api/types';
import { pipelineCancelable, pipelineMovableToTop } from '~/lib/pipeline';

function pipeline(status: string, workflowStates: string[] = []): Pipeline {
  return {
    status,
    workflows: workflowStates.map((state) => ({ state })),
  } as unknown as Pipeline;
}

describe('pipelineMovableToTop', () => {
  it('moves a pipeline with pending workflows', () => {
    expect(pipelineMovableToTop(pipeline('pending', ['pending']))).toBe(true);
    expect(pipelineMovableToTop(pipeline('running', ['running', 'pending']))).toBe(true);
  });

  it('does not move a pipeline with nothing pending', () => {
    expect(pipelineMovableToTop(pipeline('running', ['running', 'success']))).toBe(false);
    expect(pipelineMovableToTop(pipeline('pending'))).toBe(false);
  });

  it('does not move an ended or blocked pipeline', () => {
    expect(pipelineMovableToTop(pipeline('killed', ['pending']))).toBe(false);
    expect(pipelineMovableToTop(pipeline('blocked', ['pending']))).toBe(false);
    expect(pipelineMovableToTop(undefined)).toBe(false);
  });
});

describe('pipelineCancelable', () => {
  it('cancels a pending or running pipeline', () => {
    expect(pipelineCancelable(pipeline('pending'))).toBe(true);
    expect(pipelineCancelable(pipeline('running'))).toBe(true);
  });

  it('cancels a killed pipeline again while its finalizers run', () => {
    expect(pipelineCancelable(pipeline('killed', ['killed', 'running']))).toBe(true);
    expect(pipelineCancelable(pipeline('killed', ['killed', 'skipped']))).toBe(false);
  });

  it('does not cancel an ended pipeline', () => {
    expect(pipelineCancelable(pipeline('success', ['success']))).toBe(false);
    expect(pipelineCancelable(undefined)).toBe(false);
  });
});
