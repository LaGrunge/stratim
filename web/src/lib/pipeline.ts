import type { Pipeline, PipelineWorkflow } from '~/lib/api/types';

export function workflowsWithErrors(pipeline?: Pipeline): PipelineWorkflow[] {
  return pipeline?.workflows?.filter((workflow) => workflow.error !== undefined && workflow.error !== '') ?? [];
}

export function anyStepStarted(pipeline?: Pipeline): boolean {
  return (
    pipeline?.workflows?.some((workflow) =>
      workflow.children?.some((step) => step.started !== undefined && step.started > 0),
    ) ?? false
  );
}

export function pipelineHasNonWarningErrors(pipeline?: Pipeline): boolean {
  return pipeline?.errors?.some((e) => !e.is_warning) ?? false;
}

export function pipelineHasErrorsToShow(pipeline?: Pipeline): boolean {
  return pipelineHasNonWarningErrors(pipeline) || workflowsWithErrors(pipeline).length > 0;
}

// pipelineCancelable reports whether a pipeline can be canceled: it is pending or
// running, or killed while its workflows still run their finalizers, which
// canceling it again stops.
export function pipelineCancelable(pipeline?: Pipeline): boolean {
  if (pipeline === undefined) {
    return false;
  }
  if (pipeline.status === 'pending' || pipeline.status === 'running') {
    return true;
  }
  return (
    pipeline.status === 'killed' &&
    (pipeline.workflows?.some((workflow) => workflow.state === 'running' || workflow.state === 'pending') ?? false)
  );
}
