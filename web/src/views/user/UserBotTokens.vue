<template>
  <Settings
    :title="$t('user.settings.bot_tokens.bot_tokens')"
    :description="$t('user.settings.bot_tokens.desc')"
    docs-url="docs/usage/bot-tokens"
  >
    <template #headerActions>
      <Button
        v-if="!creating"
        start-icon="plus"
        :text="$t('user.settings.bot_tokens.create')"
        data-test="bot-token-new"
        @click="startCreating"
      />
    </template>

    <div v-if="created" class="mb-6 space-y-2" data-test="bot-token-created">
      <Warning :text="$t('user.settings.bot_tokens.shown_once')" />
      <pre class="code-box" data-test="bot-token-secret">{{ created.token }}</pre>
      <pre class="code-box">{{ usage }}</pre>
      <div class="flex gap-2">
        <Button :text="$t('user.settings.bot_tokens.copy')" @click="copySecret" />
        <Button color="green" :text="$t('user.settings.bot_tokens.done')" @click="created = undefined" />
      </div>
    </div>

    <form v-if="creating" class="mb-6" data-test="bot-token-form" @submit.prevent="createToken">
      <InputField v-slot="{ id }" :label="$t('user.settings.bot_tokens.name')">
        <TextField :id="id" v-model="form.name" :placeholder="$t('user.settings.bot_tokens.name')" required />
      </InputField>

      <InputField :label="$t('user.settings.bot_tokens.scopes')">
        <CheckboxesField v-model="form.scopes" :options="scopeOptions" />
      </InputField>

      <InputField :label="$t('user.settings.bot_tokens.repos')">
        <span class="text-wp-text-alt-100 mb-2 ml-1">{{
          isAdmin ? $t('user.settings.bot_tokens.repos_admin') : $t('user.settings.bot_tokens.repos_desc')
        }}</span>
        <CheckboxesField v-model="form.repos" :options="repoOptions" />
      </InputField>

      <InputField v-slot="{ id }" :label="$t('user.settings.bot_tokens.expires_in')">
        <SelectField :id="id" v-model="form.days" :options="expiryOptions" />
      </InputField>

      <div class="flex gap-2">
        <Button type="button" color="gray" :text="$t('cancel')" @click="creating = false" />
        <Button
          type="submit"
          color="green"
          :disabled="!canCreate"
          :is-loading="isCreating"
          :text="$t('user.settings.bot_tokens.create')"
        />
      </div>
    </form>

    <div class="text-wp-text-100 space-y-4">
      <ListItem
        v-for="token in tokens"
        :key="token.id"
        class="bg-wp-background-200! dark:bg-wp-background-200! flex-wrap items-center gap-2"
        data-test="bot-token"
      >
        <span class="font-bold">{{ token.name }}</span>
        <code class="code-box-inline">{{ shown(token) }}</code>
        <Badge v-for="scope in token.scopes" :key="scope" :value="scope" />
        <Badge :value="statusText(token)" />
        <span class="text-wp-text-alt-100 text-sm">{{ repoNames(token) }}</span>
        <span class="text-wp-text-alt-100 ml-auto text-sm">
          {{ $t('user.settings.bot_tokens.expires', { date: date(token.expires) }) }}
        </span>
        <span class="text-wp-text-alt-100 text-sm">
          {{
            token.last_used
              ? $t('user.settings.bot_tokens.last_used', { date: date(token.last_used) })
              : $t('user.settings.bot_tokens.never_used')
          }}
        </span>
        <IconButton
          v-if="token.status === 'active'"
          icon="trash"
          class="hover:text-wp-error-100 h-8 w-8"
          :title="$t('user.settings.bot_tokens.revoke')"
          :is-loading="isRevoking"
          data-test="bot-token-revoke"
          @click="revokeToken(token)"
        />
      </ListItem>
      <div v-if="tokens.length === 0" class="ml-2">{{ $t('user.settings.bot_tokens.none') }}</div>
    </div>
  </Settings>
</template>

<script lang="ts" setup>
import { computed, onMounted, reactive, ref } from 'vue';
import { useI18n } from 'vue-i18n';

import Badge from '~/components/atomic/Badge.vue';
import Button from '~/components/atomic/Button.vue';
import IconButton from '~/components/atomic/IconButton.vue';
import ListItem from '~/components/atomic/ListItem.vue';
import Warning from '~/components/atomic/Warning.vue';
import CheckboxesField from '~/components/form/CheckboxesField.vue';
import InputField from '~/components/form/InputField.vue';
import SelectField from '~/components/form/SelectField.vue';
import TextField from '~/components/form/TextField.vue';
import Settings from '~/components/layout/Settings.vue';
import useApiClient from '~/compositions/useApiClient';
import { useAsyncAction } from '~/compositions/useAsyncAction';
import useAuthentication from '~/compositions/useAuthentication';
import useConfig from '~/compositions/useConfig';
import useNotifications from '~/compositions/useNotifications';
import { useWPTitle } from '~/compositions/useWPTitle';
import type { BotScope, BotToken, BotTokenCreated, Repo } from '~/lib/api/types';

const apiClient = useApiClient();
const notifications = useNotifications();
const { t } = useI18n();
const { user } = useAuthentication();
const isAdmin = computed(() => user?.admin === true);

const tokens = ref<BotToken[]>([]);
const repos = ref<Repo[]>([]);
const created = ref<BotTokenCreated>();
const creating = ref(false);

const form = reactive({ name: '', scopes: ['read'] as string[], repos: [] as string[], days: '30' });

const scopeOptions = computed(() => [
  { value: 'read', text: 'read', description: t('user.settings.bot_tokens.scope_read') },
  { value: 'restart', text: 'restart', description: t('user.settings.bot_tokens.scope_restart') },
]);
const repoOptions = computed(() => repos.value.map((repo) => ({ value: String(repo.id), text: repo.full_name })));
const expiryOptions = computed(() =>
  ['7', '30', '90', '365'].map((days) => ({ value: days, text: t('user.settings.bot_tokens.days', { days }) })),
);
// an admin's token has to name its repos
const canCreate = computed(
  () => form.name.trim() !== '' && form.scopes.length > 0 && (!isAdmin.value || form.repos.length > 0),
);

const { rootPath } = useConfig();
const address = `${window.location.protocol}//${window.location.host}${rootPath}`; // port is included in location.host
const usage = computed(() => `curl -H "X-Stratim-Token: ${created.value?.token ?? ''}" "${address}/api/v2/whoami"`);

function notifyError(error: Error) {
  notifications.notifyError(error);
}

async function loadTokens() {
  tokens.value = (await apiClient.getBotTokens()) ?? [];
}

onMounted(async () => {
  await loadTokens();
  repos.value = (await apiClient.getRepoList()) ?? [];
});

function startCreating() {
  Object.assign(form, { name: '', scopes: ['read'], repos: [], days: '30' });
  created.value = undefined;
  creating.value = true;
}

const { doSubmit: createToken, isLoading: isCreating } = useAsyncAction(async () => {
  created.value = await apiClient.createBotToken({
    name: form.name.trim(),
    scopes: form.scopes as BotScope[],
    repo_ids: form.repos.map(Number),
    expires_in_days: Number(form.days),
  });
  creating.value = false;
  notifications.notify({ title: t('user.settings.bot_tokens.created'), type: 'success' });
  await loadTokens();
}, notifyError);

const { doSubmit: revokeToken, isLoading: isRevoking } = useAsyncAction(async (token: BotToken) => {
  // TODO: use proper dialog
  // eslint-disable-next-line no-alert
  if (!confirm(t('user.settings.bot_tokens.revoke_confirm', { name: token.name }))) {
    return;
  }
  await apiClient.revokeBotToken(token.id);
  notifications.notify({ title: t('user.settings.bot_tokens.revoked'), type: 'success' });
  await loadTokens();
}, notifyError);

async function copySecret() {
  if (created.value) {
    await navigator.clipboard?.writeText(created.value.token);
  }
}

function date(unix: number): string {
  return new Date(unix * 1000).toLocaleDateString();
}

// the start of a token, to tell it
function shown(token: BotToken): string {
  return `${token.prefix}…`;
}

function statusText(token: BotToken): string {
  switch (token.status) {
    case 'active':
      return t('user.settings.bot_tokens.status.active');
    case 'expired':
      return t('user.settings.bot_tokens.status.expired');
    default:
      return t('user.settings.bot_tokens.status.revoked');
  }
}

function repoNames(token: BotToken): string {
  if (token.repo_ids.length === 0) {
    return t('user.settings.bot_tokens.all_repos');
  }
  return token.repo_ids.map((id) => repos.value.find((repo) => repo.id === id)?.full_name ?? `#${id}`).join(', ');
}

useWPTitle(computed(() => [t('user.settings.bot_tokens.bot_tokens'), t('user.settings.settings')]));
</script>
