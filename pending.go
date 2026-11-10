package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// Silo sends a finished play once as a playback stop. When that delivery
// fails, Silo sends the play again later as an ordinary watched mark, which
// looks the same as an item marked watched by hand. The plugin therefore
// remembers the finished plays it saw but did not finish applying, so the
// later mark is applied even when manual marks are not synced.
const (
	pendingStateKey = "pending-completions.v1"
	// maxPendingEntries keeps the encoded set well under Silo's 256 KiB limit
	// for one instance state value.
	maxPendingEntries   = 2000
	pendingMaxAge       = 30 * 24 * time.Hour
	pendingLoadInterval = 30 * time.Second
	pendingStateTimeout = 5 * time.Second
)

type persistedPending struct {
	// Entries maps "<AniList user ID>:<watch history ID>" to the Unix time
	// the finished play was first seen.
	Entries map[string]int64 `json:"entries"`
}

// pendingCompletions is the set of finished plays not yet applied. It is kept
// in Silo's instance state so it survives a plugin restart; when that state
// cannot be read or written the set lives in memory only.
type pendingCompletions struct {
	mu      sync.Mutex
	entries map[string]int64
	// removed holds keys dropped before the saved set was read, so reading it
	// does not bring them back.
	removed    map[string]bool
	store      func() instanceStore
	now        func() time.Time
	loaded     bool
	lastLoadAt time.Time
	dirty      bool
	// saveMu serializes writes so an older copy never lands after a newer one.
	saveMu sync.Mutex
}

func newPendingCompletions() *pendingCompletions {
	return &pendingCompletions{store: hostInstanceStore, now: time.Now}
}

// pendingKey names a finished play of one AniList account, or is empty when
// the event carries no watch history ID.
func pendingKey(userID int, event *pluginv1.WatchSyncEvent) string {
	historyID := event.GetWatchHistoryId()
	if historyID == "" {
		return ""
	}
	return strconv.Itoa(userID) + ":" + historyID
}

func (p *pendingCompletions) clock() time.Time {
	if p.now == nil {
		return time.Now()
	}
	return p.now()
}

// lockLoaded locks the set, first merging the saved copy when it has not been
// read yet. A failed read is retried later.
func (p *pendingCompletions) lockLoaded(ctx context.Context) {
	p.mu.Lock()
	if p.loaded || p.store == nil || p.clock().Sub(p.lastLoadAt) < pendingLoadInterval {
		return
	}
	store := p.store()
	if store == nil {
		return
	}
	p.lastLoadAt = p.clock()
	p.mu.Unlock()
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pendingStateTimeout)
	data, found, err := store.ReadInstanceState(readCtx, pendingStateKey)
	cancel()
	p.mu.Lock()
	if p.loaded {
		return
	}
	if err != nil {
		slog.Warn("pending playback completions could not be read; keeping them in memory", "error", err)
		return
	}
	p.loaded = true
	var saved persistedPending
	if found && json.Unmarshal(data, &saved) == nil {
		for key, at := range saved.Entries {
			if _, ok := p.entries[key]; ok || p.removed[key] {
				continue
			}
			if p.entries == nil {
				p.entries = make(map[string]int64)
			}
			p.entries[key] = at
		}
	}
	p.removed = nil
	p.pruneLocked()
}

// pruneLocked drops entries older than pendingMaxAge, then the oldest beyond
// maxPendingEntries.
func (p *pendingCompletions) pruneLocked() {
	cutoff := p.clock().Add(-pendingMaxAge).Unix()
	for key, at := range p.entries {
		if at < cutoff {
			delete(p.entries, key)
			p.dirty = true
		}
	}
	if len(p.entries) <= maxPendingEntries {
		return
	}
	keys := make([]string, 0, len(p.entries))
	for key := range p.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if p.entries[keys[i]] != p.entries[keys[j]] {
			return p.entries[keys[i]] > p.entries[keys[j]]
		}
		return keys[i] < keys[j]
	})
	for _, key := range keys[maxPendingEntries:] {
		delete(p.entries, key)
	}
	p.dirty = true
}

// add records finished plays as pending and saves the set before returning,
// so a plugin that stops during the AniList requests still knows them.
func (p *pendingCompletions) add(ctx context.Context, keys []string) {
	if p == nil || len(keys) == 0 {
		return
	}
	p.lockLoaded(ctx)
	now := p.clock().Unix()
	for _, key := range keys {
		if _, ok := p.entries[key]; ok {
			continue
		}
		if p.entries == nil {
			p.entries = make(map[string]int64)
		}
		p.entries[key] = now
		delete(p.removed, key)
		p.dirty = true
	}
	p.pruneLocked()
	p.mu.Unlock()
	p.save(ctx)
}

// has reports whether a finished play is still pending.
func (p *pendingCompletions) has(ctx context.Context, key string) bool {
	if p == nil || key == "" {
		return false
	}
	p.lockLoaded(ctx)
	defer p.mu.Unlock()
	at, ok := p.entries[key]
	return ok && at >= p.clock().Add(-pendingMaxAge).Unix()
}

// remove forgets finished plays whose outcome is final.
func (p *pendingCompletions) remove(ctx context.Context, keys []string) {
	if p == nil || len(keys) == 0 {
		return
	}
	p.lockLoaded(ctx)
	for _, key := range keys {
		if _, ok := p.entries[key]; ok {
			delete(p.entries, key)
			p.dirty = true
		}
		if !p.loaded && p.store != nil {
			if p.removed == nil {
				p.removed = make(map[string]bool)
			}
			p.removed[key] = true
		}
	}
	p.mu.Unlock()
	p.save(ctx)
}

// save writes the set to Silo's instance state when it changed. Until the
// saved copy has been read nothing is written, so that copy is not replaced
// by a partial one. A failed write is logged and tried again with the next
// change.
func (p *pendingCompletions) save(ctx context.Context) {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()
	p.mu.Lock()
	var store instanceStore
	if p.store != nil {
		store = p.store()
	}
	if !p.dirty || !p.loaded || store == nil {
		p.mu.Unlock()
		return
	}
	data, err := json.Marshal(persistedPending{Entries: p.entries})
	p.dirty = false
	p.mu.Unlock()
	if err == nil {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pendingStateTimeout)
		err = store.WriteInstanceState(writeCtx, pendingStateKey, data)
		cancel()
	}
	if err != nil {
		p.mu.Lock()
		p.dirty = true
		p.mu.Unlock()
		slog.Warn("pending playback completions could not be saved; keeping them in memory", "error", err)
	}
}

// pendingResultIsFinal reports whether an event's result ends its finished
// play: it was applied, needed no change, or was rejected for good. A retry
// keeps the play pending.
func pendingResultIsFinal(result *pluginv1.WatchSyncApplyResult) bool {
	switch result.GetStatus() {
	case pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED:
		return true
	}
	return false
}
