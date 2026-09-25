# Silo AniList Sync

Syncs completed anime playback from Silo to AniList and imports mapped AniList watch history into Silo.

Part of [Crowquillx Silo Plugins](https://github.com/crowquillx/crowquillx-silo-plugins#install). Supports Linux amd64, Linux arm64 and macOS arm64.

## Install

1. Add the [Crowquillx catalog](https://github.com/crowquillx/crowquillx-silo-plugins#install) in **Administration → Plugins → Catalog** and install **AniList Sync**.
2. In **Installed**, open the plugin's **Configure** button or settings gear, then **Global Configuration**. Match **Playback completion threshold** to Silo's watched percentage and select **Save AniList settings**.
3. In each Silo profile you want to sync, open **Settings → Watch Providers → AniList → Connect**. Follow the activation link, approve access on AniList, and return to Silo.
4. Choose your watched-history import settings for that connection. Enable **Sync manually marked watched items** in the plugin settings if manual marks should also update AniList.

To restrict exports to anime already on your list, enable **Only update anime already on AniList** and add titles to AniList before watching.

For players that report completed playback at position zero, use **AniList with Silo watched check** instead. Connect it with an [AniList access token](https://anilist.co/api/v2/oauth/authorize?client_id=49797&response_type=token), a Silo server URL reachable from the plugin, a Silo API key and the same profile's ID from `GET /api/v1/profiles`. Disconnect the ordinary AniList connection for that profile after connecting the replacement.

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
