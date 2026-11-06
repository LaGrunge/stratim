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

export type BotScope = 'read' | 'restart';

export type BotTokenStatus = 'active' | 'expired' | 'revoked';

// A bot token as the API shows it: never its secret.
export interface BotToken {
  id: number;
  owner_id: number;
  name: string;
  // the start of the token, to tell it
  prefix: string;
  scopes: BotScope[];
  // the repos it is restricted to; none is all those of its owner
  repo_ids: number[];
  // unix seconds; 0 is never
  created: number;
  expires: number;
  last_used: number;
  revoked: number;
  status: BotTokenStatus;
}

// A bot token just created, with its secret: the only time it is told.
export interface BotTokenCreated extends BotToken {
  token: string;
}

export interface BotTokenRequest {
  name: string;
  scopes: BotScope[];
  repo_ids: number[];
  expires_in_days: number;
}
