import { mount } from '@vue/test-utils';
import { describe, expect, it } from 'vitest';
import { createI18n } from 'vue-i18n';

import en from '~/assets/locales/en.json';
import PipelineStatusIcon from '~/components/repo/pipeline/PipelineStatusIcon.vue';

const i18n = createI18n({
  legacy: false,
  locale: 'en',
  fallbackLocale: 'en',
  missingWarn: false,
  fallbackWarn: false,
  messages: { en },
});

describe('pipelineStatusIcon', () => {
  it('shows a timeout as an error with its own title', () => {
    const wrapper = mount(PipelineStatusIcon, {
      props: { status: 'timeout' },
      global: { plugins: [i18n] },
    });

    expect(wrapper.find('svg').exists()).toBe(true);
    expect(wrapper.find('svg').classes()).toContain('text-wp-error-100');
    expect(wrapper.attributes('title')).toBe('Status: timed out');
  });
});
