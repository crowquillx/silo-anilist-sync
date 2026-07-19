# Silo AniList Sync

An AniList watch-sync provider for [Silo Server](https://github.com/Silo-Server/silo-server).

> **Development status:** the `feat/watch-sync-provider` branch targets the new
> `watch_sync_provider.v1` SDK contract proposed in
> [Silo-Server/silo-plugin-sdk#10](https://github.com/Silo-Server/silo-plugin-sdk/pull/10).
> It is not compatible with a stock Silo release until the corresponding SDK
> and server support land. The v0.1.x releases use the legacy event-consumer
> workaround and only observe explicit “mark watched” actions.

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
3. Maps TVDB/TMDB/IMDb movie and episode identity through the daily
   [AniBridge v3 mappings](https://github.com/anibridge/anibridge-mappings).
4. Reads the existing AniList list entry and applies absolute, monotonic
   progress through `SaveMediaListEntry`.
5. Returns typed applied, rejected, retry, rate-limit, and credential outcomes
   to Silo's durable worker.

Credentials and OAuth flow data are transient RPC inputs. The plugin does not
persist or log them.

## Mapping policy

- Resolve every available TVDB and TMDB descriptor and require convergence.
- Use IMDb only as a movie fallback; AniBridge has no populated IMDb-series
  mappings.
- Support AniBridge v3 ratio semantics.
- Reject conflicting mappings, non-contiguous target ranges, and mapped
  progress beyond AniList's known episode count.
- Never guess by title.
- Retain the last valid in-memory dataset if a daily refresh fails.

## AniList progress policy

- Never lower remote progress.
- Existing completed entries never regress.
- Planning entries become current after a watched event.
- Current entries become completed at the known final episode.
- Paused, dropped, and repeating states are preserved.
- Unwatch synchronization is not advertised in the initial capability.
- Duplicate desired-state events are convergent; they never increment repeat
  count.

## Development

Requires Go 1.26 and a workspace containing the SDK capability branch:

```fish
cd /path/to/repos

go work init ./silo-anilist-sync ./silo-plugin-sdk
cd silo-anilist-sync
CGO_ENABLED=0 go test ./...
CGO_ENABLED=0 go build -o plugin .
./plugin manifest | jq
```

On NixOS:

```fish
nix shell nixpkgs#go --command fish -c 'CGO_ENABLED=0 go test ./...'
```

## Privacy and upstream services

The plugin sends AniList media IDs and absolute progress to AniList and
retrieves the public AniBridge mapping artifact from GitHub Releases. Silo
passes decrypted credentials only over the local plugin gRPC channel for calls
that need them.

## Attribution

Cross-provider mappings are supplied by
[anibridge/anibridge-mappings](https://github.com/anibridge/anibridge-mappings),
distributed under the MIT License and assembled from upstream sources
identified by that project.

## License

MIT
