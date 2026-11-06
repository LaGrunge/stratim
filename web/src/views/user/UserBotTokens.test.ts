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

import { flushPromises, mount } from '@vue/test-utils';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createI18n } from 'vue-i18n';

import en from '~/assets/locales/en.json';
import type { BotToken, BotTokenCreated, BotTokenRequest, Repo } from '~/lib/api/types';

import UserBotTokens from './UserBotTokens.vue';

const auth: { user: { admin: boolean } } = vi.hoisted(() => ({ user: { admin: false } }));

const getBotTokens = vi.fn<() => Promise<BotToken[]>>();
const createBotToken = vi.fn<(request: BotTokenRequest) => Promise<BotTokenCreated>>();
const revokeBotToken = vi.fn<(id: number) => Promise<unknown>>();
const getRepoList = vi.fn<() => Promise<Repo[]>>();

vi.mock('~/compositions/useApiClient', () => ({
  default: () => ({ getBotTokens, createBotToken, revokeBotToken, getRepoList }),
}));
vi.mock('~/compositions/useAuthentication', () => ({
  default: () => ({ user: auth.user }),
}));
vi.mock('~/compositions/useNotifications', () => ({
  default: () => ({ notify: vi.fn(), notifyError: vi.fn() }),
}));

// the labels come from the real locale file, so that a renamed key fails
const i18n = createI18n({ legacy: false, locale: 'en', messages: { en } });

const token = (id: number, name: string, extra: Partial<BotToken> = {}): BotToken => ({
  id,
  owner_id: 1,
  name,
  prefix: `stb_${name.slice(0, 8)}`,
  scopes: ['read'],
  repo_ids: [],
  created: 1_800_000_000,
  expires: 1_802_592_000,
  last_used: 0,
  revoked: 0,
  status: 'active',
  ...extra,
});

const repos = [
  { id: 4, full_name: 'org/one' },
  { id: 9, full_name: 'org/two' },
] as Repo[];

async function mountTokens() {
  const wrapper = mount(UserBotTokens, { global: { plugins: [i18n], stubs: { 'router-link': true } } });
  await flushPromises();
  return wrapper;
}

const buttonOf = (wrapper: Awaited<ReturnType<typeof mountTokens>>, text: string) =>
  wrapper.findAll('button').find((button) => button.text() === text)!;

describe('userBotTokens', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    auth.user = { admin: false };
    getRepoList.mockResolvedValue(repos);
    getBotTokens.mockResolvedValue([
      token(1, 'watcher', { repo_ids: [9], last_used: 1_800_000_100 }),
      token(2, 'old', { status: 'revoked', revoked: 1_800_000_200 }),
    ]);
  });

  it('lists the tokens, their scopes, repos and status, never a secret', async () => {
    const wrapper = await mountTokens();
    const rows = wrapper.findAll('[data-test="bot-token"]');
    expect(rows).toHaveLength(2);
    expect(rows[0].text()).toContain('watcher');
    expect(rows[0].text()).toContain('stb_watcher…');
    expect(rows[0].text()).toContain('org/two');
    expect(rows[0].text()).toContain('active');
    expect(rows[1].text()).toContain('revoked');
    expect(rows[1].text()).toContain('never used');
    // only an active token can be revoked
    expect(rows[0].find('[data-test="bot-token-revoke"]').exists()).toBe(true);
    expect(rows[1].find('[data-test="bot-token-revoke"]').exists()).toBe(false);
    expect(wrapper.find('[data-test="bot-token-secret"]').exists()).toBe(false);
  });

  it('creates a token and shows its secret once', async () => {
    createBotToken.mockResolvedValue({ ...token(3, 'agent', { scopes: ['read', 'restart'] }), token: 'stb_secret' });
    const wrapper = await mountTokens();

    await wrapper.find('[data-test="bot-token-new"]').trigger('click');
    const form = wrapper.find('[data-test="bot-token-form"]');
    await form.find('input:not([type="checkbox"])').setValue(' agent ');
    const checkboxes = form.findAll('input[type="checkbox"]');
    // read (checked by default), restart, org/one, org/two
    expect(checkboxes).toHaveLength(4);
    await checkboxes[1].trigger('click');
    await checkboxes[2].trigger('click');
    await form.find('select').setValue('90');
    await form.trigger('submit');
    await flushPromises();

    expect(createBotToken).toHaveBeenCalledWith({
      name: 'agent',
      scopes: ['read', 'restart'],
      repo_ids: [4],
      expires_in_days: 90,
    });
    expect(wrapper.find('[data-test="bot-token-form"]').exists()).toBe(false);
    expect(wrapper.find('[data-test="bot-token-secret"]').text()).toBe('stb_secret');
    expect(wrapper.text()).toContain('X-Stratim-Token: stb_secret');
    expect(getBotTokens).toHaveBeenCalledTimes(2);

    await buttonOf(wrapper, 'Done').trigger('click');
    expect(wrapper.find('[data-test="bot-token-secret"]').exists()).toBe(false);
  });

  it('makes an admin name the repos of a token', async () => {
    auth.user = { admin: true };
    const wrapper = await mountTokens();

    await wrapper.find('[data-test="bot-token-new"]').trigger('click');
    const form = wrapper.find('[data-test="bot-token-form"]');
    expect(form.text()).toContain('The token of an admin has to name its repositories.');
    await form.find('input:not([type="checkbox"])').setValue('admin-bot');
    const submit = form.find('button[type="submit"]');
    expect(submit.attributes('disabled')).toBeDefined();

    await form.findAll('input[type="checkbox"]')[3].trigger('click');
    expect(form.find('button[type="submit"]').attributes('disabled')).toBeUndefined();
  });

  it('revokes a token once confirmed', async () => {
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
    const wrapper = await mountTokens();

    await wrapper.find('[data-test="bot-token-revoke"]').trigger('click');
    await flushPromises();

    expect(confirm).toHaveBeenCalledWith('Revoke the bot token watcher? Programs using it lose access.');
    expect(revokeBotToken).toHaveBeenCalledWith(1);
    expect(getBotTokens).toHaveBeenCalledTimes(2);
    confirm.mockRestore();
  });
});
