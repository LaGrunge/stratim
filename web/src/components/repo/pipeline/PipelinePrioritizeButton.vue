<template>
  <Button
    v-if="user?.admin && pipelineMovableToTop(pipeline)"
    class="shrink-0"
    :text="$t('repo.pipeline.actions.prioritize')"
    :title="$t('repo.pipeline.actions.prioritize_desc')"
    :is-loading="isPrioritizing"
    data-test="pipeline-prioritize"
    @click="emit('prioritize')"
  />
</template>

<script lang="ts" setup>
import Button from '~/components/atomic/Button.vue';
import useAuthentication from '~/compositions/useAuthentication';
import type { Pipeline } from '~/lib/api/types';
import { pipelineMovableToTop } from '~/lib/pipeline';

defineProps<{
  pipeline: Pipeline;
  isPrioritizing?: boolean;
}>();

const emit = defineEmits<{
  (event: 'prioritize'): void;
}>();

// moving a pipeline to the top of the queue is a server admin's action
const { user } = useAuthentication();
</script>
