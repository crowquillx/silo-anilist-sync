package main

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/crowquillx/silo-anilist-sync/anilist"
)

// The status page's data is kept in Silo's per-instance plugin state, so it
// survives restarts. Silo caps one value at 256 KiB; the limits below keep
// the encoded status well under that.
const (
	statusStateKey     = "status.v1"
	maxStatusBytes     = 200 << 10
	maxAccounts        = 25
	maxUnmappedTitles  = 20
	maxProblems        = 50
	maxStatusTextBytes = 300
	statusSaveDelay    = 5 * time.Second
	statusLoadInterval = 30 * time.Second
)

// instanceStore is the part of Silo's runtime host the status store uses.
type instanceStore interface {
	ReadInstanceState(ctx context.Context, key string) ([]byte, bool, error)
	WriteInstanceState(ctx context.Context, key string, value []byte) error
}

// hostInstanceStore returns Silo's instance state, or nil before the plugin
// is connected to the host and in tests.
func hostInstanceStore() instanceStore {
	if host := sdkruntime.Host(); host != nil {
		return host
	}
	return nil
}

type persistedStatus struct {
	Accounts map[string]*accountStatus `json:"accounts,omitempty"`
	Problems []problem                 `json:"problems,omitempty"`
	Rescan   rescanState               `json:"rescan"`
}

// accountStatus is what the plugin has seen of one AniList account. The
// plugin cannot tell which Silo profile a connection belongs to, so the page
// groups by AniList account.
type accountStatus struct {
	UserID         int       `json:"user_id"`
	Name           string    `json:"name,omitempty"`
	ProfileURL     string    `json:"profile_url,omitempty"`
	Capability     string    `json:"capability,omitempty"`
	TokenExpiresAt time.Time `json:"token_expires_at,omitzero"`
	LastSeenAt     time.Time `json:"last_seen_at,omitzero"`

	NeedsReconnect bool      `json:"needs_reconnect,omitempty"`
	ReconnectAt    time.Time `json:"reconnect_at,omitzero"`

	LastExportAt time.Time    `json:"last_export_at,omitzero"`
	Exports      exportCounts `json:"exports"`

	LastImportAt   time.Time       `json:"last_import_at,omitzero"`
	ImportFull     bool            `json:"import_full,omitempty"`
	ImportEntries  int             `json:"import_entries,omitempty"`
	ImportStates   int             `json:"import_states,omitempty"`
	ImportRatings  int             `json:"import_ratings,omitempty"`
	UnmappedCount  int             `json:"unmapped_count,omitempty"`
	UnmappedTitles []unmappedTitle `json:"unmapped_titles,omitempty"`

	LastError   string    `json:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitzero"`
}

type exportCounts struct {
	Applied  int `json:"applied"`
	NoChange int `json:"no_change"`
	Rejected int `json:"rejected"`
	Retried  int `json:"retried"`
}

type unmappedTitle struct {
	MediaID int    `json:"media_id"`
	Title   string `json:"title"`
	Format  string `json:"format,omitempty"`
}

// problem is one export that AniList did not apply.
type problem struct {
	At     time.Time `json:"at"`
	UserID int       `json:"user_id,omitempty"`
	Item   string    `json:"item"`
	Status string    `json:"status"`
	Reason string    `json:"reason"`
}

// rescanState is a request to read every account's whole list once. Each
// account that finished a full import after RequestedAt is done.
type rescanState struct {
	RequestedAt time.Time            `json:"requested_at,omitzero"`
	Done        map[string]time.Time `json:"done,omitempty"`
	// SettingOn is the last seen value of the global full-scan setting, so
	// turning it on starts a new rescan.
	SettingOn bool `json:"setting_on,omitempty"`
}

type statusStore struct {
	mu         sync.Mutex
	state      persistedStatus
	store      func() instanceStore
	now        func() time.Time
	loaded     bool
	lastLoadAt time.Time
	saveTimer  *time.Timer
}

func newStatusStore() *statusStore {
	return &statusStore{store: hostInstanceStore, now: time.Now}
}

// lockLoaded locks the store, first merging the saved status from Silo when
// it has not been read yet. A failed read is retried later; changes made in
// the meantime are kept.
func (s *statusStore) lockLoaded(ctx context.Context) {
	s.mu.Lock()
	if s.loaded || s.store == nil || s.now().Sub(s.lastLoadAt) < statusLoadInterval {
		return
	}
	store := s.store()
	if store == nil {
		return
	}
	s.lastLoadAt = s.now()
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	data, found, err := store.ReadInstanceState(ctx, statusStateKey)
	cancel()
	s.mu.Lock()
	if s.loaded || err != nil {
		return
	}
	s.loaded = true
	if !found {
		return
	}
	var saved persistedStatus
	if json.Unmarshal(data, &saved) != nil {
		return
	}
	s.state = mergeStatus(saved, s.state)
}

// mergeStatus lays the status gathered before the saved copy was read over
// that copy.
func mergeStatus(saved, current persistedStatus) persistedStatus {
	if saved.Accounts == nil {
		saved.Accounts = make(map[string]*accountStatus)
	}
	for key, account := range current.Accounts {
		saved.Accounts[key] = account
	}
	saved.Problems = append(current.Problems, saved.Problems...)
	if len(saved.Problems) > maxProblems {
		saved.Problems = saved.Problems[:maxProblems]
	}
	if !current.Rescan.RequestedAt.IsZero() || current.Rescan.SettingOn {
		saved.Rescan = current.Rescan
	}
	return saved
}

// update applies fn to the status and schedules a save.
func (s *statusStore) update(ctx context.Context, fn func(*persistedStatus)) {
	if s == nil {
		return
	}
	s.lockLoaded(ctx)
	defer s.mu.Unlock()
	if s.state.Accounts == nil {
		s.state.Accounts = make(map[string]*accountStatus)
	}
	fn(&s.state)
	s.trimLocked()
	if s.saveTimer == nil && s.store != nil {
		s.saveTimer = time.AfterFunc(statusSaveDelay, s.save)
	}
}

// account returns the status of an AniList account, creating it. Callers
// hold the lock through update.
func (p *persistedStatus) account(userID int, now time.Time) *accountStatus {
	key := strconv.Itoa(userID)
	account := p.Accounts[key]
	if account == nil {
		account = &accountStatus{UserID: userID}
		p.Accounts[key] = account
	}
	account.LastSeenAt = now
	return account
}

func (s *statusStore) trimLocked() {
	if len(s.state.Problems) > maxProblems {
		s.state.Problems = s.state.Problems[:maxProblems]
	}
	if len(s.state.Accounts) <= maxAccounts {
		return
	}
	accounts := s.sortedAccountsLocked()
	for _, account := range accounts[maxAccounts:] {
		delete(s.state.Accounts, strconv.Itoa(account.UserID))
	}
}

func (s *statusStore) sortedAccountsLocked() []*accountStatus {
	accounts := make([]*accountStatus, 0, len(s.state.Accounts))
	for _, account := range s.state.Accounts {
		accounts = append(accounts, account)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if !accounts[i].LastSeenAt.Equal(accounts[j].LastSeenAt) {
			return accounts[i].LastSeenAt.After(accounts[j].LastSeenAt)
		}
		return accounts[i].UserID < accounts[j].UserID
	})
	return accounts
}

func (s *statusStore) save() {
	s.mu.Lock()
	s.saveTimer = nil
	data := s.encodeLocked()
	var store instanceStore
	if s.store != nil {
		store = s.store()
	}
	s.mu.Unlock()
	if store == nil || data == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = store.WriteInstanceState(ctx, statusStateKey, data)
}

// encodeLocked encodes the status, dropping the least useful detail until it
// fits Silo's limit.
func (s *statusStore) encodeLocked() []byte {
	state := s.state
	for {
		data, err := json.Marshal(state)
		if err != nil {
			return nil
		}
		if len(data) <= maxStatusBytes {
			return data
		}
		switch {
		case len(state.Problems) > 0:
			state.Problems = state.Problems[:len(state.Problems)/2]
		default:
			trimmed := make(map[string]*accountStatus, len(state.Accounts))
			dropped := false
			for key, account := range state.Accounts {
				copied := *account
				if len(copied.UnmappedTitles) > 0 {
					copied.UnmappedTitles = nil
					dropped = true
				}
				trimmed[key] = &copied
			}
			if !dropped {
				return nil
			}
			state.Accounts = trimmed
		}
	}
}

// snapshot returns a copy of the status for the page.
func (s *statusStore) snapshot(ctx context.Context) (persistedStatus, []*accountStatus) {
	if s == nil {
		return persistedStatus{}, nil
	}
	s.lockLoaded(ctx)
	defer s.mu.Unlock()
	accounts := s.sortedAccountsLocked()
	copies := make([]*accountStatus, len(accounts))
	for i, account := range accounts {
		copied := *account
		copied.UnmappedTitles = append([]unmappedTitle(nil), account.UnmappedTitles...)
		copies[i] = &copied
	}
	state := s.state
	state.Problems = append([]problem(nil), s.state.Problems...)
	return state, copies
}

func truncateText(text string) string {
	if len(text) <= maxStatusTextBytes {
		return text
	}
	cut := maxStatusTextBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…"
}

// The methods below record what the watch-sync calls observe.

func (s *statusStore) recordAccount(ctx context.Context, capability string, account anilist.Account, token string) {
	s.update(ctx, func(p *persistedStatus) {
		status := p.account(account.ID, s.now())
		status.Name, status.ProfileURL = account.Name, account.ProfileURL
		if capability != "" {
			status.Capability = capability
		}
		status.TokenExpiresAt = anilist.TokenExpiresAt(token)
		status.NeedsReconnect = false
		status.ReconnectAt = time.Time{}
	})
}

// recordCall notes a watch-sync call for an account and whether AniList
// rejected its token.
func (s *statusStore) recordCall(ctx context.Context, userID int, capability, token string, invalidToken bool, failure string) {
	if userID < 1 {
		return
	}
	s.update(ctx, func(p *persistedStatus) {
		now := s.now()
		status := p.account(userID, now)
		if capability != "" {
			status.Capability = capability
		}
		if expires := anilist.TokenExpiresAt(token); !expires.IsZero() {
			status.TokenExpiresAt = expires
		}
		switch {
		case invalidToken:
			if !status.NeedsReconnect {
				status.ReconnectAt = now
			}
			status.NeedsReconnect = true
		case failure == "":
			status.NeedsReconnect = false
			status.ReconnectAt = time.Time{}
		}
		if failure != "" {
			status.LastError, status.LastErrorAt = truncateText(failure), now
		}
	})
}

type exportOutcome struct {
	item   string
	status string
	reason string
}

func (s *statusStore) recordExports(ctx context.Context, userID int, outcomes []exportOutcome) {
	if len(outcomes) == 0 {
		return
	}
	s.update(ctx, func(p *persistedStatus) {
		now := s.now()
		var account *accountStatus
		if userID > 0 {
			account = p.account(userID, now)
			account.LastExportAt = now
		}
		var problems []problem
		for _, outcome := range outcomes {
			if account != nil {
				switch outcome.status {
				case "applied":
					account.Exports.Applied++
				case "no change":
					account.Exports.NoChange++
				case "rejected":
					account.Exports.Rejected++
				case "retry":
					account.Exports.Retried++
				}
			}
			if outcome.status == "rejected" || outcome.status == "retry" {
				problems = append(problems, problem{At: now, UserID: userID, Item: truncateText(outcome.item), Status: outcome.status, Reason: truncateText(outcome.reason)})
			}
		}
		// Newest first.
		for i, j := 0, len(problems)-1; i < j; i, j = i+1, j-1 {
			problems[i], problems[j] = problems[j], problems[i]
		}
		p.Problems = append(problems, p.Problems...)
	})
}

type importSummary struct {
	full     bool
	entries  int
	states   int
	ratings  int
	unmapped []anilist.ListEntry
}

func (s *statusStore) recordImport(ctx context.Context, userID int, summary importSummary, rating bool) {
	if userID < 1 {
		return
	}
	s.update(ctx, func(p *persistedStatus) {
		now := s.now()
		account := p.account(userID, now)
		account.NeedsReconnect = false
		if rating {
			account.ImportRatings = summary.ratings
			return
		}
		account.LastImportAt = now
		account.ImportFull = summary.full
		account.ImportEntries = summary.entries
		account.ImportStates = summary.states
		// An incremental import sees only changed entries, so it can add to
		// the unmapped list but not show that a title is mapped now.
		if summary.full {
			account.UnmappedCount = len(summary.unmapped)
			account.UnmappedTitles = nil
		}
		known := make(map[int]bool, len(account.UnmappedTitles))
		for _, title := range account.UnmappedTitles {
			known[title.MediaID] = true
		}
		for _, entry := range summary.unmapped {
			if known[entry.Media.ID] {
				continue
			}
			known[entry.Media.ID] = true
			if !summary.full {
				account.UnmappedCount++
			}
			if len(account.UnmappedTitles) < maxUnmappedTitles {
				account.UnmappedTitles = append(account.UnmappedTitles, unmappedTitle{
					MediaID: entry.Media.ID, Title: truncateText(entry.PreferredTitle()), Format: entry.Media.Format,
				})
			}
		}
		if summary.full {
			if p.Rescan.Done == nil {
				p.Rescan.Done = make(map[string]time.Time)
			}
			p.Rescan.Done[strconv.Itoa(userID)] = now
		}
	})
}

// fullScanDue reports whether an account's next import should read its
// whole list, starting a new rescan when the global setting was just turned
// on.
func (s *statusStore) fullScanDue(ctx context.Context, userID int, settingOn bool) bool {
	if s == nil {
		return settingOn
	}
	due := false
	s.update(ctx, func(p *persistedStatus) {
		if settingOn && !p.Rescan.SettingOn {
			p.Rescan.RequestedAt = s.now()
			p.Rescan.Done = nil
		}
		p.Rescan.SettingOn = settingOn
		if p.Rescan.RequestedAt.IsZero() {
			return
		}
		done := p.Rescan.Done[strconv.Itoa(userID)]
		due = done.Before(p.Rescan.RequestedAt)
	})
	return due
}

func (s *statusStore) requestRescan(ctx context.Context) {
	s.update(ctx, func(p *persistedStatus) {
		p.Rescan.RequestedAt = s.now()
		p.Rescan.Done = nil
	})
}

func (s *statusStore) clearProblems(ctx context.Context) {
	s.update(ctx, func(p *persistedStatus) {
		p.Problems = nil
		for _, account := range p.Accounts {
			account.Exports = exportCounts{}
			account.LastError, account.LastErrorAt = "", time.Time{}
		}
	})
}
