package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"strconv"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	pluginID = "dev.crowquillx.anilist-sync"
	// tokenWarning is how long before a token expires the page warns.
	tokenWarning = 30 * 24 * time.Hour
	// recentThrottle is how long a 429 from AniList stays on the page.
	recentThrottle = time.Hour
)

type notice struct {
	Warn bool
	Text string
}

type accountRow struct {
	Name          string
	URL           string
	Provider      string
	Status        string
	StatusWarn    bool
	TokenExpires  string
	TokenWarn     bool
	LastExport    string
	Exports       exportCounts
	LastImport    string
	ImportDetail  string
	UnmappedCount int
	Unmapped      []unmappedRow
	LastError     string
	LastErrorAt   string
}

type unmappedRow struct {
	Title, Format, URL string
}

type problemRow struct {
	At, Account, Item, Status, Reason string
}

type mappingRow struct {
	Name, Loaded, Entries, Problem string
	Warn                           bool
}

type pageData struct {
	PluginID string
	Version  string
	Saved    string
	Notices  []notice
	Accounts []accountRow
	Problems []problemRow
	Mappings []mappingRow
	Rate     string
	Rescan   string
}

var statusPage = template.Must(template.New("status").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>AniList Sync</title><style>body{font:16px system-ui,sans-serif;line-height:1.5;max-width:1200px;margin:32px auto;padding:0 24px;color:#20242b;background:#fafafa}h1{font-size:28px}h2{font-size:20px;margin-top:32px}table{border-collapse:collapse;width:100%;background:white}th,td{text-align:left;vertical-align:top;padding:10px 12px;border-bottom:1px solid #ddd}th{font-weight:600}p,ul{max-width:75ch}small{color:#555}.box{background:white;border:1px solid #ddd;border-left:4px solid #b54708;padding:12px 16px;margin:12px 0}.ok{border-left-color:#067647}.warn{color:#b54708;font-weight:600}form{display:inline}button{font:inherit;padding:6px 14px;margin:4px 8px 4px 0;border:1px solid #888;border-radius:6px;background:white;cursor:pointer}details{margin:4px 0}summary{cursor:pointer}</style>
<h1>AniList Sync</h1>
<p><a href="/admin/plugins/{{.PluginID}}">Settings</a> · <a href="/">Back to Silo</a> · <small>Version {{.Version}}</small></p>
{{if .Saved}}<div class="box ok">{{.Saved}}</div>{{end}}
{{range .Notices}}<div class="box{{if not .Warn}} ok{{end}}">{{.Text}}</div>{{end}}
<h2>Connected accounts</h2>
{{if .Accounts}}<p>One row per AniList account the plugin has seen. Silo does not tell plugins which profile a connection belongs to; each profile sees its own connection in Settings → Watch Providers.</p>
<table><thead><tr><th>Account</th><th>Status</th><th>Sent to AniList</th><th>Imported</th></tr></thead><tbody>{{range .Accounts}}<tr>
<td>{{if .URL}}<a href="{{.URL}}" target="_blank" rel="noopener noreferrer">{{.Name}}</a>{{else}}{{.Name}}{{end}}<br><small>{{.Provider}}</small></td>
<td>{{if .StatusWarn}}<span class="warn">{{.Status}}</span>{{else}}{{.Status}}{{end}}<br><small>{{if .TokenWarn}}<span class="warn">{{.TokenExpires}}</span>{{else}}{{.TokenExpires}}{{end}}</small>{{if .LastError}}<br><small>Last problem {{.LastErrorAt}}: {{.LastError}}</small>{{end}}</td>
<td>{{if .LastExport}}{{.LastExport}}<br><small>{{.Exports.Applied}} applied, {{.Exports.NoChange}} already up to date, {{.Exports.Rejected}} rejected, {{.Exports.Retried}} to retry</small>{{else}}<small>Nothing yet</small>{{end}}</td>
<td>{{if .LastImport}}{{.LastImport}}<br><small>{{.ImportDetail}}</small>{{else}}<small>Nothing yet</small>{{end}}
{{if .UnmappedCount}}<details><summary>{{.UnmappedCount}} not matched</summary><small>No TVDB, TMDB or IMDb mapping covers these yet, so their history was not imported. Mappings come from AniBridge and update daily.</small><ul>{{range .Unmapped}}<li><a href="{{.URL}}" target="_blank" rel="noopener noreferrer">{{.Title}}</a>{{if .Format}} <small>{{.Format}}</small>{{end}}</li>{{end}}</ul>{{if gt .UnmappedCount (len .Unmapped)}}<small>The first {{len .Unmapped}} are shown.</small>{{end}}</details>{{end}}</td>
</tr>{{end}}</tbody></table>
{{else}}<p>No AniList connection has synced yet. Each profile connects in Settings → Watch Providers → AniList.</p>{{end}}
<h2>Recent problems</h2>
{{if .Problems}}<p>Exports AniList did not apply, newest first. Rejected items are not sent again; items to retry are sent on the next sync.</p>
<table><thead><tr><th>When</th><th>Account</th><th>Item</th><th>Result</th><th>Reason</th></tr></thead><tbody>{{range .Problems}}<tr><td><small>{{.At}}</small></td><td>{{.Account}}</td><td>{{.Item}}</td><td>{{.Status}}</td><td><small>{{.Reason}}</small></td></tr>{{end}}</tbody></table>
<form method="post" action="clear-problems"><button type="submit">Clear problems and counts</button></form>
{{else}}<p>None.</p>{{end}}
<h2>Full rescan</h2>
<p>Imports normally read only AniList entries changed since the last sync. A rescan reads each account's whole list once, on its next sync, for example after mappings improved.</p>
<p>{{.Rescan}}</p>
<form method="post" action="rescan"><button type="submit">Rescan all accounts on their next sync</button></form>
<h2>Mapping data</h2>
<table><thead><tr><th>Dataset</th><th>Loaded</th><th>Entries</th><th>Problem</th></tr></thead><tbody>{{range .Mappings}}<tr><td>{{.Name}}</td><td>{{.Loaded}}</td><td>{{.Entries}}</td><td>{{if .Warn}}<span class="warn">{{.Problem}}</span>{{else}}<small>{{.Problem}}</small>{{end}}</td></tr>{{end}}</tbody></table>
<h2>AniList requests</h2>
<p>{{.Rate}}</p>
</html>`))

func (s *server) Handle(ctx context.Context, req *pluginv1.HandleHTTPRequest) (*pluginv1.HandleHTTPResponse, error) {
	method, path := req.GetMethod(), req.GetPath()
	switch {
	case method == "GET" && path == "/":
		return s.renderPage(ctx, savedMessage(query(req.GetQuery(), "saved")))
	case method == "GET" && path == "/status":
		state, accounts := s.status.snapshot(ctx)
		body, err := json.Marshal(struct {
			Version  string                 `json:"version"`
			Accounts []*accountStatus       `json:"accounts"`
			Problems []problem              `json:"problems"`
			Rescan   rescanState            `json:"rescan"`
			Mappings []mapping.SourceStatus `json:"mappings"`
			AniList  anilist.RateStatus     `json:"anilist"`
		}{pluginVersion(), accounts, state.Problems, state.Rescan, s.mappingStatus(), anilist.SharedRateStatus()})
		if err != nil {
			return nil, err
		}
		return pageResponse(200, "application/json", body), nil
	case method == "POST" && path == "/rescan":
		s.status.requestRescan(ctx)
		return redirect("./?saved=rescan"), nil
	case method == "POST" && path == "/clear-problems":
		s.status.clearProblems(ctx)
		return redirect("./?saved=cleared"), nil
	case path == "/" || path == "/status" || path == "/rescan" || path == "/clear-problems":
		return &pluginv1.HandleHTTPResponse{StatusCode: 405}, nil
	}
	return &pluginv1.HandleHTTPResponse{StatusCode: 404}, nil
}

func savedMessage(saved string) string {
	switch saved {
	case "rescan":
		return "Every account reads its whole AniList list on its next sync."
	case "cleared":
		return "Problems and counts were cleared."
	}
	return ""
}

func (s *server) mappingStatus() []mapping.SourceStatus {
	if s.mappings == nil {
		return nil
	}
	return s.mappings.Status()
}

func (s *server) renderPage(ctx context.Context, saved string) (*pluginv1.HandleHTTPResponse, error) {
	now := time.Now()
	state, accounts := s.status.snapshot(ctx)
	data := pageData{PluginID: pluginID, Version: pluginVersion(), Saved: saved}
	names := make(map[int]string, len(accounts))
	for _, account := range accounts {
		row := accountPageRow(account, now)
		names[account.UserID] = row.Name
		data.Accounts = append(data.Accounts, row)
		switch {
		case account.NeedsReconnect:
			data.Notices = append(data.Notices, notice{true, fmt.Sprintf("AniList rejected the saved token of %s %s. That profile must reconnect AniList in Settings → Watch Providers; Silo keeps its plays until then.", row.Name, relative(account.ReconnectAt, now))})
		case !account.TokenExpiresAt.IsZero() && account.TokenExpiresAt.Before(now):
			data.Notices = append(data.Notices, notice{true, fmt.Sprintf("The AniList token of %s expired %s. That profile must reconnect AniList in Settings → Watch Providers.", row.Name, relative(account.TokenExpiresAt, now))})
		case !account.TokenExpiresAt.IsZero() && account.TokenExpiresAt.Sub(now) < tokenWarning:
			data.Notices = append(data.Notices, notice{true, fmt.Sprintf("The AniList token of %s expires %s. Reconnect AniList in Settings → Watch Providers before then to keep syncing.", row.Name, relative(account.TokenExpiresAt, now))})
		}
	}
	for _, source := range s.mappingStatus() {
		row := mappingRow{Name: source.Name, Loaded: "Not yet", Entries: "–"}
		if !source.LoadedAt.IsZero() {
			row.Loaded, row.Entries = relative(source.LoadedAt, now), strconv.Itoa(source.Entries)
		}
		if source.LastError != "" {
			row.Warn = true
			row.Problem = fmt.Sprintf("Download failed %s: %s. Next attempt %s.", relative(source.ErrorAt, now), source.LastError, relative(source.RetryAt, now))
			if source.LoadedAt.IsZero() {
				data.Notices = append(data.Notices, notice{true, fmt.Sprintf("%s mapping data could not be downloaded, so exports that need it wait. Next attempt %s.", source.Name, relative(source.RetryAt, now))})
			}
		}
		data.Mappings = append(data.Mappings, row)
	}
	rate := anilist.SharedRateStatus()
	data.Rate = "No AniList requests yet."
	if rate.Limit > 0 {
		data.Rate = fmt.Sprintf("AniList allows %d requests a minute; the plugin spaces them %s apart, shared by every account.", rate.Limit, rate.Interval.Round(100*time.Millisecond))
	}
	if rate.Throttled > 0 {
		data.Rate += fmt.Sprintf(" AniList asked the plugin to slow down %d times since it started, last %s.", rate.Throttled, relative(rate.ThrottledAt, now))
		if now.Sub(rate.ThrottledAt) < recentThrottle {
			data.Notices = append(data.Notices, notice{true, "AniList limited requests in the last hour. Exports slow down and resume on their own."})
		}
	}
	data.Rescan = rescanText(state.Rescan, len(accounts), now)
	for _, p := range state.Problems {
		account := names[p.UserID]
		if account == "" {
			account = accountName(p.UserID, "")
		}
		data.Problems = append(data.Problems, problemRow{At: relative(p.At, now), Account: account, Item: p.Item, Status: p.Status, Reason: p.Reason})
	}
	var body bytes.Buffer
	if err := statusPage.Execute(&body, data); err != nil {
		return nil, err
	}
	return pageResponse(200, "text/html; charset=utf-8", body.Bytes()), nil
}

func accountPageRow(account *accountStatus, now time.Time) accountRow {
	row := accountRow{Name: accountName(account.UserID, account.Name), URL: account.ProfileURL, Provider: providerName(account.Capability)}
	switch {
	case account.NeedsReconnect:
		row.Status, row.StatusWarn = "Needs reconnect", true
	case !account.TokenExpiresAt.IsZero() && account.TokenExpiresAt.Before(now):
		row.Status, row.StatusWarn = "Token expired", true
	default:
		row.Status = "Connected, seen " + relative(account.LastSeenAt, now)
	}
	if !account.TokenExpiresAt.IsZero() {
		row.TokenExpires = "Token expires " + relative(account.TokenExpiresAt, now)
		row.TokenWarn = account.TokenExpiresAt.Sub(now) < tokenWarning
	}
	if !account.LastExportAt.IsZero() {
		row.LastExport, row.Exports = relative(account.LastExportAt, now), account.Exports
	}
	if !account.LastImportAt.IsZero() {
		row.LastImport = relative(account.LastImportAt, now)
		scope := "changed entries"
		if account.ImportFull {
			scope = "full list"
		}
		row.ImportDetail = fmt.Sprintf("%s: %d entries, %d watched items", scope, account.ImportEntries, account.ImportStates)
		if account.ImportRatings > 0 {
			row.ImportDetail += fmt.Sprintf(", %d movie ratings", account.ImportRatings)
		}
	}
	row.UnmappedCount = account.UnmappedCount
	for _, title := range account.UnmappedTitles {
		row.Unmapped = append(row.Unmapped, unmappedRow{Title: title.Title, Format: title.Format, URL: fmt.Sprintf("https://anilist.co/anime/%d", title.MediaID)})
	}
	if account.LastError != "" {
		row.LastError, row.LastErrorAt = account.LastError, relative(account.LastErrorAt, now)
	}
	return row
}

func accountName(userID int, name string) string {
	if name != "" {
		return name
	}
	if userID > 0 {
		return fmt.Sprintf("AniList user %d", userID)
	}
	return "Unknown account"
}

func providerName(capability string) string {
	if capability == siloVerifiedCapability {
		return "AniList with Silo watched check"
	}
	return "AniList"
}

func rescanText(rescan rescanState, accounts int, now time.Time) string {
	text := "No rescan was requested."
	if !rescan.RequestedAt.IsZero() {
		done := 0
		for _, at := range rescan.Done {
			if !at.Before(rescan.RequestedAt) {
				done++
			}
		}
		text = fmt.Sprintf("Last requested %s; %d of %d accounts have rescanned since.", relative(rescan.RequestedAt, now), done, accounts)
	}
	if rescan.SettingOn {
		text += " Full import rescan is on in the plugin settings: each account rescans once. Turn it off and on to rescan again."
	}
	return text
}

// relative renders a time as a date with a rough distance from now.
func relative(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := now.Sub(t)
	future := d < 0
	if future {
		d = -d
	}
	var amount string
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		amount = plural(int(d/time.Minute), "minute")
	case d < 48*time.Hour:
		amount = plural(int(d/time.Hour), "hour")
	default:
		amount = plural(int(d/(24*time.Hour)), "day")
	}
	date := t.UTC().Format("2006-01-02 15:04 UTC")
	if future {
		return fmt.Sprintf("in %s (%s)", amount, date)
	}
	return fmt.Sprintf("%s ago (%s)", amount, date)
}

func plural(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

func pluginVersion() string {
	if version != "" {
		return version
	}
	var m struct {
		Version string `json:"version"`
	}
	_ = json.Unmarshal(manifestJSON, &m)
	return m.Version
}

func query(q *structpb.Struct, key string) string {
	switch v := q.GetFields()[key].GetKind().(type) {
	case *structpb.Value_StringValue:
		return v.StringValue
	case *structpb.Value_ListValue:
		if values := v.ListValue.GetValues(); len(values) > 0 {
			return values[0].GetStringValue()
		}
	}
	return ""
}

func pageResponse(status int32, contentType string, body []byte) *pluginv1.HandleHTTPResponse {
	return &pluginv1.HandleHTTPResponse{StatusCode: status, Headers: map[string]string{
		"Content-Type":            contentType,
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'self'",
		"X-Content-Type-Options":  "nosniff",
	}, Body: body}
}

// redirect answers a form post. The location is relative to the posted
// route, so it stays on the plugin's page whatever prefix Silo serves it at.
func redirect(location string) *pluginv1.HandleHTTPResponse {
	return &pluginv1.HandleHTTPResponse{StatusCode: 303, Headers: map[string]string{"Location": location, "Cache-Control": "no-store"}}
}
