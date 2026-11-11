# Silo AniList Sync

Syncs anime watch progress and movie ratings from Silo to AniList, and imports mapped AniList watch history and movie ratings into Silo.

Part of [Crowquillx Silo Plugins](https://github.com/crowquillx/crowquillx-silo-plugins#install). Supports Linux amd64, Linux arm64 and macOS arm64. Requires a Silo server from September 24, 2026 or later; per-profile settings need one from October 12, 2026 or later.

## Install

1. Add the [Crowquillx catalog](https://github.com/crowquillx/crowquillx-silo-plugins#install) in **Administration → Plugins → Catalog** and install **AniList Sync**.
2. In each Silo profile you want to sync, open **Settings → Watch Providers → AniList → Connect**. Follow the activation link, approve access on AniList, and return to Silo.
3. Choose that connection's sync options. Besides Silo's own import and export switches, each profile can turn on **Send manually marked watched items** and **Only update anime already on my AniList list**.

The plugin's **Global Configuration** has the same two switches for every profile. With **Only update anime already on AniList** on, add titles to Plan to Watch before watching.

For players that report completed playback at position zero, use **AniList with Silo watched check** instead. Connect it with an [AniList access token](https://anilist.co/api/v2/oauth/authorize?client_id=49797&response_type=token), a Silo server URL reachable from the plugin, a Silo API key and the same profile's ID from `GET /api/v1/profiles`. Disconnect the ordinary AniList connection for that profile after connecting the replacement.

## What syncs

- **Watch progress.** Finished episodes and movies advance AniList progress, never lower it. Silo's own watched threshold decides when a play is finished. A finished play that could not be sent, for example while AniList was down, is sent with Silo's next sync. Two cases are not covered. A play that finished while the plugin was not running reaches it later as an ordinary watched mark, which is sent only with **Send manually marked watched items** on. And a play waiting to be sent belongs to the AniList account it was watched under, so connecting a different account drops it. Watching a planned, paused or dropped anime moves it to Watching, and the last episode completes it. Finishing a rewatch completes it and counts the rewatch.
- **Movie ratings.** Ratings sync both ways for movies, converted between Silo's 1 to 10 and AniList's 100-point scores. Series ratings and dropped shows are not synced, because one Silo series usually spans several AniList entries.
- **History imports.** AniList progress is mapped back to TVDB, TMDB or IMDb episodes and movies through AniBridge. After the first sync, imports read only entries changed since the last one.

AniList tokens last about a year. When AniList rejects a token, Silo keeps the profile's plays and its Watch Providers page says to reconnect AniList.

## AniList Sync page

Administrators find **AniList Sync** in the admin menu. It shows:

- each connected AniList account, with token expiry, what was last sent and imported, and anything that needs a reconnect,
- AniList entries no mapping covers, so their history was not imported,
- recent exports AniList rejected or that will be retried, with the reason,
- the age of the mapping data and failed downloads,
- AniList's request limit and recent throttling.

**Rescan all accounts on their next sync** reads every account's whole list once, for example after mappings improved. The **Full import rescan** setting does the same when turned on.

Silo does not tell plugins which profile a connection belongs to, so the page lists AniList accounts, not profiles.

## Build

Requires Go 1.26 or newer.

```sh
make test
make build
```

The binary is `plugin`.

## Acknowledgments

- [AniList](https://anilist.co/) for list tracking and its API.
- [AniBridge](https://github.com/anibridge/anibridge-mappings), [Anime-Lists](https://github.com/Anime-Lists/anime-lists) and [ARM](https://github.com/BeeeQueue/arm-server) for anime mappings.
- [AniList Connect Bridge](https://github.com/crowquillx/anilist-connect-bridge) for account activation.
- [Silo plugin SDK](https://github.com/Silo-Server/silo-plugin-sdk) for Silo integration.

[MIT license](LICENSE).
