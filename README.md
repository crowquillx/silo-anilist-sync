# Silo AniList Sync

An AniList watch-sync provider for [Silo Server](https://github.com/Silo-Server/silo-server).

> **Development status:** this branch targets the `watch_sync_provider.v1`
> contract on the main branches of
> [`silo-server`](https://github.com/Silo-Server/silo-server) and
> [`silo-plugin-sdk`](https://github.com/Silo-Server/silo-plugin-sdk), including
> the additive v0.13 provider operations. A Silo release containing the merged
> plugin-provider host adapter is still required.

## Architecture

Silo owns:

- AniList OAuth application secrets and callback state
- encrypted credentials scoped to a Silo user and profile
- durable desired-state events and stable event IDs
- delivery ordering, retries, rate-limit deferral, and reconciliation
- rich movie/episode identity captured at watch completion

The plugin remains a stateless provider adapter. It:

1. Builds AniList authorization URLs and exchanges authorization codes.
2. Validates credentials with AniList's `Viewer` query.
3. Imports AniList watch history by expanding list progress into completed
   movies and episodes and reverse-mapping them through AniBridge.
4. Maps Silo TVDB/TMDB/IMDb movie and episode identity through the daily
   [AniBridge v3 mappings](https://github.com/anibridge/anibridge-mappings),
   falling back through [Anime-Lists](https://github.com/Anime-Lists/anime-lists)
   and the [ARM mapping service](https://github.com/BeeeQueue/arm-server) when
   AniBridge has no direct export mapping.
5. Advances AniList on completed playback stop events. Manual watched marks use
   the same convergent update only when their separate plugin toggle is enabled.
6. Reads the existing AniList list entry and applies absolute, monotonic
   progress through `SaveMediaListEntry`.
7. Returns typed applied, no-change, rejected, retry, rate-limit, and credential
   outcomes to Silo's durable worker.

Credentials and OAuth flow data are transient RPC inputs. The plugin does not
persist or log them.

## Mapping policy

- Resolve every available TVDB and TMDB descriptor and require convergence.
- Use IMDb only as a movie fallback; AniBridge has no populated IMDb-series
  mappings.
- Prefer direct AniBridge mappings. Use Anime-Lists only to resolve an external
  identity and episode to AniDB. Prefer AniBridge's AniDB episode projection
  when available, then use ARM as the independent AniDB-to-AniList fallback for
  regular episodes and movies.
- Support AniBridge v3 ratio semantics.
- Reject conflicting mappings, non-contiguous target ranges, and mapped
  progress beyond AniList's known episode count.
- Never guess by title.
- Import only identities that AniBridge can reverse-map; title-only matches and
  export-only Anime-Lists/ARM fallbacks are not safe for history import.
- Retain the last valid in-memory datasets if a daily refresh fails.

## AniList progress policy

- Never lower remote progress.
- Existing completed entries never regress.
- Planning entries become current after a watched event.
- Current entries become completed at the known final episode.
- Paused, dropped, and repeating states are preserved.
- Import each mapped episode up to AniList's absolute progress as watched.
- Sync completed playback after it passes the configured completion threshold,
  which defaults to Silo's default of 90%.
- Ignore start, pause, and incomplete stop events because AniList has no
  intra-episode scrobble state.
- Do not sync history reconciliation or manually marked watched items unless
  **Sync manually marked watched items** is enabled.
- Do not advertise resume-progress import, unwatch, favorites, or watchlists
  where AniList cannot preserve Silo's item-level semantics without destructive
  or lossy side effects.
- Duplicate desired-state events are convergent; they never increment repeat
  count.

## Development

Requires Go 1.26 and `silo-plugin-sdk` v0.13.0 or newer:

```fish
cd /path/to/silo-anilist-sync
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build -o plugin .
./plugin manifest | jq
```

On NixOS:

```fish
nix shell nixpkgs#go --command fish -c 'CGO_ENABLED=0 go test ./...'
```

## Privacy and upstream services

The plugin sends AniList media IDs and absolute progress to AniList and reads
the connected account's anime-list progress for watched imports. It retrieves
the public AniBridge and Anime-Lists mapping artifacts from GitHub; fallback
resolution sends AniDB IDs to the public ARM service. No AniList credentials or
Silo user information are sent to mapping services. Silo passes decrypted
credentials only over the local plugin gRPC channel for calls that need them.

## Attribution

Many thanks to [AniBridge](https://github.com/anibridge/anibridge-mappings),
[Anime-Lists](https://github.com/Anime-Lists/anime-lists), and
[ARM](https://github.com/BeeeQueue/arm-server) for making reliable
cross-provider anime mapping possible.

## License

MIT
