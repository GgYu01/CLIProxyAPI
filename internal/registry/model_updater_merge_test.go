package registry

import (
	"encoding/json"
	"testing"
)

func mergeTestModel(id, displayName string) *ModelInfo {
	return &ModelInfo{ID: id, Object: "model", OwnedBy: "openai", Type: "openai", DisplayName: displayName}
}

func mergeTestSection(ids ...string) []*ModelInfo {
	models := make([]*ModelInfo, 0, len(ids))
	for _, id := range ids {
		models = append(models, mergeTestModel(id, id))
	}
	return models
}

func TestMergeEmbeddedExtrasRestoresEmbeddedOnlyModels(t *testing.T) {
	previous := modelsCatalogStore.data
	t.Cleanup(func() { modelsCatalogStore.data = previous })

	modelsCatalogStore.data = &staticModelsJSON{
		CodexPro: mergeTestSection("gpt-5.4", "gpt-5.5"),
		Claude:   mergeTestSection("claude-x"),
	}

	// Mirrors the Sep 2026 feed: upstream dropped gpt-5.4 from every codex tier.
	remote := &staticModelsJSON{
		CodexPro: mergeTestSection("gpt-5.5", "gpt-6-astra"),
		Claude:   mergeTestSection("claude-x", "claude-new"),
	}

	merged := mergeEmbeddedExtras(remote)
	if merged == remote {
		t.Fatal("merge must not mutate or alias the remote catalog in place")
	}

	gotIDs := map[string]bool{}
	for _, model := range merged.CodexPro {
		gotIDs[model.ID] = true
	}
	for _, want := range []string{"gpt-5.4", "gpt-5.5", "gpt-6-astra"} {
		if !gotIDs[want] {
			t.Fatalf("merged codex-pro missing %q; got %v", want, gotIDs)
		}
	}
	// Remote-only entries must stay present in sections the embedded catalog
	// does not extend, and remote ordering must be preserved up front.
	if merged.Claude[0].ID != "claude-x" || merged.Claude[1].ID != "claude-new" {
		t.Fatalf("unexpected claude section after merge: %v", merged.Claude)
	}

	// The caller's remote slice must remain untouched.
	if len(remote.CodexPro) != 2 {
		t.Fatalf("remote codex-pro mutated: %d entries", len(remote.CodexPro))
	}
}

func TestMergeEmbeddedExtrasRemoteWinsOnConflict(t *testing.T) {
	previous := modelsCatalogStore.data
	t.Cleanup(func() { modelsCatalogStore.data = previous })

	modelsCatalogStore.data = &staticModelsJSON{
		CodexPro: []*ModelInfo{mergeTestModel("gpt-5.5", "Embedded Display")},
	}

	remote := &staticModelsJSON{
		CodexPro: []*ModelInfo{mergeTestModel("gpt-5.5", "Remote Display")},
	}

	merged := mergeEmbeddedExtras(remote)
	if len(merged.CodexPro) != 1 {
		t.Fatalf("expected conflict to keep a single entry, got %d", len(merged.CodexPro))
	}
	if merged.CodexPro[0].DisplayName != "Remote Display" {
		t.Fatalf("remote definition must win on conflict, got %q", merged.CodexPro[0].DisplayName)
	}
}

func TestMergeEmbeddedExtrasHandlesNilAndEmptyEmbedded(t *testing.T) {
	previous := modelsCatalogStore.data
	t.Cleanup(func() { modelsCatalogStore.data = previous })

	remote := &staticModelsJSON{CodexPro: mergeTestSection("gpt-5.5")}

	modelsCatalogStore.data = nil
	if merged := mergeEmbeddedExtras(remote); merged != remote {
		t.Fatal("nil embedded store must return remote unchanged")
	}

	modelsCatalogStore.data = &staticModelsJSON{}
	merged := mergeEmbeddedExtras(remote)
	if len(merged.CodexPro) != 1 || merged.CodexPro[0].ID != "gpt-5.5" {
		t.Fatalf("empty embedded sections must leave remote untouched, got %+v", merged.CodexPro)
	}

	if merged := mergeEmbeddedExtras(nil); merged != nil {
		t.Fatal("nil remote must stay nil")
	}
}

func TestMergedCatalogStillValidatesAndEncodes(t *testing.T) {
	previous := modelsCatalogStore.data
	t.Cleanup(func() { modelsCatalogStore.data = previous })

	modelsCatalogStore.data = &staticModelsJSON{CodexPro: mergeTestSection("gpt-5.4", "gpt-5.5")}
	remote := &staticModelsJSON{CodexPro: mergeTestSection("gpt-5.5")}

	merged := mergeEmbeddedExtras(remote)
	if err := validateModelsCatalog(merged); err != nil {
		t.Fatalf("merged catalog failed validation: %v", err)
	}
	if _, err := json.Marshal(merged); err != nil {
		t.Fatalf("merged catalog not encodable: %v", err)
	}
}
