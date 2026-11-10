package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestEncodeCatalogManifestRoundTripsThroughSiloJSONContract(t *testing.T) {
	encoded, err := encodeCatalogManifest(filepath.Join("..", "..", "manifest.json"), "9.8.7")
	if err != nil {
		t.Fatal(err)
	}
	var manifest pluginv1.PluginManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.GetPluginId() != "dev.crowquillx.anilist-sync" || manifest.GetVersion() != "9.8.7" || manifest.GetChecksum() != "" {
		t.Fatalf("manifest identity = %q@%q checksum %q", manifest.GetPluginId(), manifest.GetVersion(), manifest.GetChecksum())
	}
	descriptor := manifest.GetCapabilities()[0].GetWatchSyncProvider()
	if len(descriptor.GetAuthMethods()) != 1 || descriptor.GetAuthMethods()[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE {
		t.Fatalf("auth methods = %v", descriptor.GetAuthMethods())
	}
	fields := manifest.GetGlobalConfigSchema()[0].GetAdminForm().GetFields()
	if len(fields) != 3 ||
		fields[0].GetControl() != pluginv1.AdminFormControl_ADMIN_FORM_CONTROL_SWITCH ||
		fields[1].GetKey() != "import_full_scan" ||
		fields[1].GetControl() != pluginv1.AdminFormControl_ADMIN_FORM_CONTROL_SWITCH ||
		fields[2].GetKey() != "only_existing_entries" ||
		fields[2].GetControl() != pluginv1.AdminFormControl_ADMIN_FORM_CONTROL_SWITCH ||
		fields[2].GetDefaultValue() == nil || fields[2].GetDefaultValue().GetBoolValue() {
		t.Fatalf("admin fields = %#v", fields)
	}
}
