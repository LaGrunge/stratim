export interface Cron {
  id: number;
  name: string;
  branch: string;
  schedule: string;
  timezone: string;
  enabled: boolean;
  next_exec: number;
  variables: Record<string, string>;
  // the workflow a cron of a workflow's config runs
  workflow?: string;
  // the cron comes from the config of the default branch: only enabling it changes here
  managed?: boolean;
}
