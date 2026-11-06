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

package datastore

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"xorm.io/xorm/schemas"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

var errNoBotTokenHash = errors.New("bot token: no hash given")

const (
	// The sizes of the text columns of bot_token_calls.
	botCallMethodSize = 10
	botCallTextSize   = 255
	botCallIPSize     = 64
	// How many calls a purge deletes at once.
	botCallPurgeBatch = 1000
)

// BotTokenCreate stores a new bot token unless its owner has maxActive
// active ones at now (unix seconds) already: types.ErrBotTokenLimit then.
// The owner's row is locked meanwhile, so that two creations at once do
// not both pass the count.
func (s storage) BotTokenCreate(token *model.BotToken, maxActive int, now int64) error {
	return retryWrite(func() error {
		sess := s.engine.NewSession()
		defer sess.Close()
		if err := sess.Begin(); err != nil {
			return lockErr(err)
		}
		owner := sess.ID(token.OwnerID)
		switch s.engine.Dialect().URI().DBType {
		case schemas.MYSQL, schemas.POSTGRES:
			owner = owner.ForUpdate()
		default: // sqlite: one write at a time
		}
		exist, err := owner.Exist(new(model.User))
		if err != nil {
			return lockErr(err)
		}
		if !exist {
			return types.ErrRecordNotExist
		}
		active, err := sess.Where("owner_id = ? AND revoked = 0 AND expires > ?", token.OwnerID, now).Count(new(model.BotToken))
		if err != nil {
			return lockErr(err)
		}
		if active >= int64(maxActive) {
			return types.ErrBotTokenLimit
		}
		if err := wrapInsert(sess.Insert(token)); err != nil {
			return err
		}
		return lockErr(sess.Commit())
	})
}

// BotTokenGet gets a bot token by its id.
func (s storage) BotTokenGet(id int64) (*model.BotToken, error) {
	token := new(model.BotToken)
	return token, wrapGet(s.engine.ID(id).Get(token))
}

// BotTokenFindByHash gets the bot token whose secret has the hash.
func (s storage) BotTokenFindByHash(hash string) (*model.BotToken, error) {
	// an empty hash would make an empty where clause
	if hash == "" {
		return nil, errNoBotTokenHash
	}
	token := new(model.BotToken)
	return token, wrapGet(s.engine.Where("token_hash = ?", hash).Get(token))
}

// BotTokenList lists the bot tokens of a user, or of all users when
// ownerID is 0, newest first.
func (s storage) BotTokenList(ownerID int64, p *model.ListOptionsWithAll) ([]*model.BotToken, error) {
	tokens := make([]*model.BotToken, 0)
	q := s.paginate(p).OrderBy("id DESC")
	if ownerID != 0 {
		q = q.Where("owner_id = ?", ownerID)
	}
	return tokens, q.Find(&tokens)
}

// BotTokenRevoke revokes a bot token at (unix seconds), unless it is
// already: it reports whether it did.
func (s storage) BotTokenRevoke(id, at int64) (bool, error) {
	var n int64
	err := retryWrite(func() error {
		var err error
		n, err = s.engine.Table(new(model.BotToken)).Where("id = ? AND revoked = 0", id).Update(map[string]any{"revoked": at})
		return wrapUpdate(n, err)
	})
	return n > 0, err
}

// BotTokenTouch records a use of a bot token at (unix seconds), unless a
// later one is.
func (s storage) BotTokenTouch(id, at int64) error {
	return retryWrite(func() error {
		return wrapUpdate(s.engine.Table(new(model.BotToken)).Where("id = ? AND last_used < ?", id, at).Update(map[string]any{"last_used": at}))
	})
}

// BotTokenCallAppend records a call made with a bot token. Its text, which
// comes from the request, is made valid UTF-8 without NUL and cut to its
// columns, so that no request fails its record.
func (s storage) BotTokenCallAppend(call *model.BotTokenCall) error {
	call.Method = cleanText(call.Method, botCallMethodSize)
	call.Route = cleanText(call.Route, botCallTextSize)
	call.Reason = cleanText(call.Reason, botCallTextSize)
	call.IP = cleanText(call.IP, botCallIPSize)
	return wrapInsert(s.engine.Insert(call))
}

// cleanText makes s valid UTF-8 without NUL, of at most n bytes, cut on a
// character.
func cleanText(s string, n int) string {
	s = strings.ReplaceAll(strings.ToValidUTF8(s, string(utf8.RuneError)), "\x00", "")
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// BotTokenCallList lists the latest calls of a bot token, at most limit,
// newest first.
func (s storage) BotTokenCallList(tokenID int64, limit int) ([]*model.BotTokenCall, error) {
	calls := make([]*model.BotTokenCall, 0, limit)
	return calls, s.engine.Where("token_id = ?", tokenID).OrderBy("created DESC, id DESC").Limit(limit).Find(&calls)
}

// BotTokenCallPurge deletes the calls made before (unix seconds), a batch
// at a time, and tells how many.
func (s storage) BotTokenCallPurge(before int64) (int64, error) {
	var total int64
	for {
		var n int64
		err := retryWrite(func() error {
			var err error
			n, err = s.purgeBotCalls(before)
			return wrapUpdate(n, err)
		})
		total += n
		if err != nil || n < botCallPurgeBatch {
			return total, err
		}
	}
}

// purgeBotCalls deletes a batch of the calls made before.
func (s storage) purgeBotCalls(before int64) (int64, error) {
	query := fmt.Sprintf("DELETE FROM bot_token_calls WHERE id IN (SELECT id FROM bot_token_calls WHERE created < ? ORDER BY id LIMIT %d)", botCallPurgeBatch)
	if s.engine.Dialect().URI().DBType == schemas.MYSQL {
		// MariaDB has no LIMIT in a subquery of IN, but one in DELETE
		query = fmt.Sprintf("DELETE FROM bot_token_calls WHERE created < ? ORDER BY id LIMIT %d", botCallPurgeBatch)
	}
	res, err := s.engine.Exec(query, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
