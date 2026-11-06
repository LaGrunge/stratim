import { mount } from '@vue/test-utils';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createI18n } from 'vue-i18n';

import PipelinePrioritizeButton from '~/components/repo/pipeline/PipelinePrioritizeButton.vue';
import type { Pipeline } from '~/lib/api/types';

const auth: { user?: { admin: boolean } } = vi.hoisted(() => ({ user: { admin: true } }));

vi.mock('~/compositions/useAuthentication', () => ({
  default: () => ({ user: auth.user }),
}));

const i18n = createI18n({
  legacy: false,
  locale: 'en',
  fallbackLocale: 'en',
  missingWarn: false,
  fallbackWarn: false,
  messages: { en: {} },
});

function mountButton(status: string, workflowStates: string[]) {
  const pipeline = { status, workflows: workflowStates.map((state) => ({ state })) } as unknown as Pipeline;
  return mount(PipelinePrioritizeButton, {
    props: { pipeline },
    global: { plugins: [i18n] },
  });
}

function shown(status: string, workflowStates: string[]): boolean {
  return mountButton(status, workflowStates).find('[data-test="pipeline-prioritize"]').exists();
}

describe('pipelinePrioritizeButton', () => {
  beforeEach(() => {
    auth.user = { admin: true };
  });

  it('moves a pipeline with pending workflows to the top', async () => {
    const wrapper = mountButton('running', ['running', 'pending']);
    const button = wrapper.find('[data-test="pipeline-prioritize"]');

    expect(button.exists()).toBe(true);
    await button.trigger('click');
    expect(wrapper.emitted('prioritize')).toHaveLength(1);
  });

  it('is not shown once nothing of the pipeline waits in the queue', () => {
    expect(shown('running', ['running', 'success'])).toBe(false);
    expect(shown('success', ['success'])).toBe(false);
    expect(shown('blocked', ['pending'])).toBe(false);
  });

  it('is shown to server admins only', () => {
    auth.user = { admin: false };
    expect(shown('pending', ['pending'])).toBe(false);
    auth.user = undefined;
    expect(shown('pending', ['pending'])).toBe(false);
  });
});
