package daemon_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/config"
	"go.kenn.io/kata/internal/daemon"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/embedding"
	mcpserver "go.kenn.io/kata/internal/mcp"
	"go.kenn.io/kata/internal/testenv"
	"go.kenn.io/kata/internal/testfix"
	"go.kenn.io/kata/internal/vector"
	kataclient "go.kenn.io/kata/pkg/client"
)

func TestDirectSpokeSearchUsesHubResults(t *testing.T) {
	ctx := t.Context()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Input []string `json:"input"`
		}
		require.NoError(t, json.UnmarshalRead(r.Body, &input))
		data := make([]map[string]any, len(input.Input))
		for i := range data {
			data[i] = map[string]any{"embedding": []float32{1, 0}}
		}
		require.NoError(t, json.MarshalWrite(w, map[string]any{"data": data}))
	}))
	t.Cleanup(provider.Close)
	emb, err := embedding.New(embedding.Config{BaseURL: provider.URL, Model: "example", Dims: 2})
	require.NoError(t, err)
	idx, err := vector.Open(ctx, filepath.Join(t.TempDir(), "vectors.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.Close()) })
	manager, err := daemon.NewWebSessionManager(daemon.WebSessionManagerConfig{Origin: "https://tasks.example", InstanceID: "hub"})
	require.NoError(t, err)
	hub := testenv.New(t, func(cfg *daemon.ServerConfig) { cfg.Embedder, cfg.VectorIndex, cfg.WebSessions = emb, idx, manager })
	project, issue := createClaimHubIssue(t, hub)
	_, err = idx.RefreshMirror(ctx, hub.DB)
	require.NoError(t, err)
	key := emb.Generation().Fingerprint()
	require.NoError(t, idx.EnsureBuilding(ctx, key, emb.Generation()))
	_, err = idx.Fill(ctx, key, func(_ context.Context, inputs []string) ([][]float32, error) {
		vectors := make([][]float32, len(inputs))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	}, 64, nil, nil)
	require.NoError(t, err)
	require.NoError(t, idx.CutOver(ctx, key))
	spoke := testenv.New(t)
	other, err := spoke.DB.CreateProject(ctx, "other-project")
	require.NoError(t, err)
	_, _, err = spoke.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: other.ID, Title: "claim local only", Author: "tester"})
	require.NoError(t, err)
	replica, err := spoke.DB.CreateProjectWithUID(ctx, project.Name, project.UID)
	require.NoError(t, err)
	_, err = spoke.DB.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: replica.ID, Role: db.FederationRoleSpoke, HubURL: hub.URL, HubProjectID: project.ID, HubProjectUID: project.UID, Actor: "tester", Enabled: true, PushEnabled: true})
	require.NoError(t, err)
	events, err := hub.DB.EventsAfter(ctx, db.EventsAfterParams{ProjectID: project.ID, IssueUID: issue.UID, Types: []string{"issue.created"}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, events, 1)
	e := events[0]
	_, err = spoke.DB.InsertRemoteEvent(ctx, replica.ID, db.RemoteEvent{EventUID: e.UID, OriginInstanceUID: e.OriginInstanceUID, ProjectUID: e.ProjectUID, ProjectName: e.ProjectName, IssueUID: e.IssueUID, RelatedIssueUID: e.RelatedIssueUID, Type: e.Type, Actor: e.Actor, HLCPhysicalMS: e.HLCPhysicalMS, HLCCounter: e.HLCCounter, ContentHash: e.ContentHash, Payload: jsontext.Value(e.Payload), CreatedAt: e.CreatedAt})
	require.NoError(t, err)
	require.NoError(t, spoke.DB.MaterializeFederatedProject(ctx, replica.ID))
	grant := createClaimEnrollment(t, hub, project.ID, spoke.DB.InstanceUID(), "pull,push")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: project.ID, Token: grant.Token}))
	filteredPath := fmt.Sprintf("/api/v1/projects/%d/federation/search?q=concept&label=missing", project.ID)
	filteredResp, filtered := envDoRaw(t, hub, http.MethodGet, filteredPath, nil, map[string]string{"Authorization": "Bearer " + grant.Token})
	require.Equal(t, http.StatusOK, filteredResp.StatusCode, string(filtered))
	require.Contains(t, string(filtered), `"results":[]`)
	hubResp, hubRaw := envDoRaw(t, hub, http.MethodGet, fmt.Sprintf("/api/v1/projects/%d/federation/search?q=concept", project.ID), nil, map[string]string{"Authorization": "Bearer " + grant.Token})
	require.Equal(t, http.StatusOK, hubResp.StatusCode, string(hubRaw))
	var hubShape struct {
		Results []map[string]jsontext.Value `json:"results"`
	}
	require.NoError(t, json.Unmarshal(hubRaw, &hubShape))
	require.Len(t, hubShape.Results, 1)
	require.Contains(t, hubShape.Results[0], "issue")
	require.Contains(t, hubShape.Results[0], "score")
	require.Contains(t, hubShape.Results[0], "web_url")
	require.NotContains(t, hubShape.Results[0], "labels")
	require.NotContains(t, hubShape.Results[0], "relationships")
	require.NotContains(t, hubShape.Results[0], "children")
	resp, raw := envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var body struct {
		Results []struct {
			Issue  db.Issue `json:"issue"`
			Remote bool     `json:"remote"`
			WebURL string   `json:"web_url"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Results, 1)
	local, err := spoke.DB.IssueByUID(ctx, issue.UID, db.IncludeDeletedNo)
	require.NoError(t, err)
	require.Equal(t, local.ID, body.Results[0].Issue.ID)
	require.Equal(t, issue.UID, body.Results[0].Issue.UID)
	require.NotEqual(t, issue.ID, local.ID)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=claim", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var hybrid struct {
		Mode    string `json:"mode"`
		Results []struct {
			MatchedIn []string `json:"matched_in"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal(raw, &hybrid))
	require.Equal(t, "hybrid", hybrid.Mode)
	require.Len(t, hybrid.Results, 1)
	require.Contains(t, hybrid.Results[0].MatchedIn, "semantic")
	require.Contains(t, hybrid.Results[0].MatchedIn, "title")
	apiClient, err := kataclient.NewWithHTTPClient(spoke.URL, spoke.HTTP)
	require.NoError(t, err)
	mcp, err := mcpserver.New(mcpserver.Options{Client: apiClient, Scope: mcpserver.NewAllScope(), Actor: "tester", Version: "test"})
	require.NoError(t, err)
	serverTransport, clientTransport := sdkmcp.NewInMemoryTransports()
	serverSession, err := mcp.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverSession.Close() })
	mcpClient := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := mcpClient.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	_, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.load_issue_discovery", Arguments: map[string]any{}})
	require.NoError(t, err)
	result, err := session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.search", Arguments: map[string]any{"query": "claim"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	output, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.Contains(t, string(output), "claim target")
	require.Contains(t, string(output), "claim local only")
	first, _, err := hub.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID, UID: "01HZNQ7VFPK1XGD8R5MABXD4EX", Title: "recent hub one", Body: "remote hub body", Author: "tester"})
	require.NoError(t, err)
	second, _, err := hub.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID, Title: "recent hub two", Author: "tester"})
	require.NoError(t, err)
	title, updatedBody := "hub updated title", "current hub body"
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: issue.ID, Title: &title, Body: &updatedBody, Actor: "tester"})
	require.NoError(t, err)
	_, err = hub.DB.AddLabel(ctx, issue.ID, "hub-label", "tester")
	require.NoError(t, err)
	_, err = idx.RefreshMirror(ctx, hub.DB)
	require.NoError(t, err)
	_, err = idx.Fill(ctx, key, func(_ context.Context, inputs []string) ([][]float32, error) {
		vectors := make([][]float32, len(inputs))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	}, 64, nil, nil)
	require.NoError(t, err)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Results, 3)
	byUID := make(map[string]int)
	for i, hit := range body.Results {
		byUID[hit.Issue.UID] = i
	}
	require.Len(t, byUID, 3)
	for _, remote := range []db.Issue{first, second} {
		i, ok := byUID[remote.UID]
		require.True(t, ok)
		hit := body.Results[i]
		require.True(t, hit.Remote)
		require.Zero(t, hit.Issue.ID)
		require.Equal(t, replica.ID, hit.Issue.ProjectID)
		require.Equal(t, remote.ShortID, hit.Issue.ShortID)
		require.Equal(t, remote.Title, hit.Issue.Title)
		require.Equal(t, remote.Body, hit.Issue.Body)
		require.Equal(t, "https://tasks.example/kata?issue="+remote.UID+"#direct=1", hit.WebURL)
	}
	known := body.Results[byUID[issue.UID]]
	require.False(t, known.Remote)
	require.Equal(t, local.ID, known.Issue.ID)
	require.Equal(t, updatedBody, known.Issue.Body)
	require.Equal(t, local.ShortID, known.Issue.ShortID)
	require.Equal(t, "hub updated title", known.Issue.Title)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic&label=hub-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Results, 1)
	require.Equal(t, issue.UID, body.Results[0].Issue.UID)
	require.Equal(t, local.ID, body.Results[0].Issue.ID)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=claim", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	found := false
	for _, hit := range body.Results {
		if hit.Issue.UID == issue.UID {
			require.Equal(t, title, hit.Issue.Title)
			require.Equal(t, updatedBody, hit.Issue.Body)
			found = true
		}
	}
	require.True(t, found)
	_, _, _, err = hub.DB.CloseIssue(ctx, issue.ID, "done", "tester", "", nil)
	require.NoError(t, err)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=claim&status=open", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	for _, hit := range body.Results {
		require.NotEqual(t, issue.UID, hit.Issue.UID, "a hub-closed issue must not appear in open results")
	}
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic&status=closed&label=hub-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Results, 1)
	require.Equal(t, "closed", body.Results[0].Issue.Status)
	require.Equal(t, local.ID, body.Results[0].Issue.ID)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic&status=open&label=hub-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Empty(t, body.Results)
	result, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.search", Arguments: map[string]any{"query": "concept"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	output, err = json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	for _, remote := range []db.Issue{first, second} {
		require.Contains(t, string(output), `"ref":"`+strings.ToLower(remote.UID)+`"`)
		require.Contains(t, string(output), `"qualified_ref":"`+project.Name+"#"+strings.ToLower(remote.UID)+`"`)
		require.Contains(t, string(output), "https://tasks.example/kata?issue="+remote.UID)
	}
	require.Contains(t, string(output), `"remote":true`)
	_, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.load_issue_mutation", Arguments: map[string]any{}})
	require.NoError(t, err)
	ref := project.Name + "#" + strings.ToLower(first.UID)
	for _, call := range []struct {
		name string
		args map[string]any
	}{
		{"kata.show", map[string]any{"ref": ref}},
		{"kata.comment", map[string]any{"ref": ref, "body": "note", "idempotency_key": "remote-before-pull"}},
	} {
		result, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: call.name, Arguments: call.args})
		require.NoError(t, err)
		require.True(t, result.IsError)
	}
	// The hub's short ID is already taken on this spoke when the issue arrives.
	colliding, _, err := spoke.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: replica.ID, UID: "01HZNQ7VFPK1XGD8R5MABCD4EX", Title: "local collision", Author: "tester"})
	require.NoError(t, err)
	require.Equal(t, "d4ex", first.ShortID)
	require.Equal(t, first.ShortID, colliding.ShortID)
	events, err = hub.DB.EventsAfter(ctx, db.EventsAfterParams{ProjectID: project.ID, IssueUID: first.UID, Types: []string{"issue.created"}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, events, 1)
	e = events[0]
	_, err = spoke.DB.InsertRemoteEvent(ctx, replica.ID, db.RemoteEvent{EventUID: e.UID, OriginInstanceUID: e.OriginInstanceUID, ProjectUID: e.ProjectUID, ProjectName: e.ProjectName, IssueUID: e.IssueUID, RelatedIssueUID: e.RelatedIssueUID, Type: e.Type, Actor: e.Actor, HLCPhysicalMS: e.HLCPhysicalMS, HLCCounter: e.HLCCounter, ContentHash: e.ContentHash, Payload: jsontext.Value(e.Payload), CreatedAt: e.CreatedAt})
	require.NoError(t, err)
	require.NoError(t, spoke.DB.MaterializeFederatedProject(ctx, replica.ID))
	pulled, err := spoke.DB.IssueByUID(ctx, first.UID, db.IncludeDeletedNo)
	require.NoError(t, err)
	require.Equal(t, "xd4ex", pulled.ShortID)
	result, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.show", Arguments: map[string]any{"ref": ref}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	output, err = json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.Contains(t, string(output), first.UID)
	result, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.comment", Arguments: map[string]any{"ref": ref, "body": "note", "idempotency_key": "remote-after-pull"}})
	require.NoError(t, err)
	require.False(t, result.IsError, "%+v", result.Content)
	output, err = json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.Contains(t, string(output), first.UID)

	var bigBody, oversized db.Issue
	for i := range 197 {
		created, _, err := hub.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID, Title: fmt.Sprintf("large corpus %03d", i), Author: "tester"})
		require.NoError(t, err)
		if i == 0 {
			bigBody = created
		}
		if i == 1 {
			oversized = created
		}
	}
	_, err = idx.RefreshMirror(ctx, hub.DB)
	require.NoError(t, err)
	_, err = idx.Fill(ctx, key, func(_ context.Context, inputs []string) ([][]float32, error) {
		vectors := make([][]float32, len(inputs))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	}, 0, nil, nil)
	require.NoError(t, err)
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: bigBody.ID, Body: new(strings.Repeat("x", 9<<20)), Actor: "tester"})
	require.NoError(t, err)
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: oversized.ID, Title: new(strings.Repeat("z", 65<<10)), Actor: "tester"})
	require.NoError(t, err)
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: issue.ID, Body: new(strings.Repeat("z", 65<<10)), Actor: "tester"})
	require.NoError(t, err)
	_, err = hub.DB.AddLabel(ctx, oversized.ID, "oversized", "tester")
	require.NoError(t, err)
	path := fmt.Sprintf("/api/v1/projects/%d/federation/search?q=concept&limit=200", project.ID)
	resp, raw = envDoRaw(t, hub, http.MethodGet, path, nil, map[string]string{"Authorization": "Bearer " + grant.Token})
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var hubRows struct {
		Results []struct {
			Issue struct {
				UID string `json:"uid"`
			} `json:"issue"`
			WebURL    string `json:"web_url"`
			Oversized bool   `json:"oversized"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal(raw, &hubRows))
	require.Len(t, hubRows.Results, 200)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic&limit=200", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var spokeRows struct {
		Results []struct {
			Issue     db.Issue `json:"issue"`
			Oversized bool     `json:"oversized"`
			WebURL    string   `json:"web_url"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal(raw, &spokeRows))
	require.Len(t, spokeRows.Results, 200)
	oversizedFound := false
	for i, row := range hubRows.Results {
		require.Equal(t, row.Issue.UID, spokeRows.Results[i].Issue.UID)
		if row.Issue.UID == issue.UID {
			require.True(t, row.Oversized)
			require.True(t, spokeRows.Results[i].Oversized)
			require.Equal(t, local.ID, spokeRows.Results[i].Issue.ID)
		}
		if row.Issue.UID == oversized.UID {
			oversizedFound = true
			require.True(t, row.Oversized)
			require.True(t, spokeRows.Results[i].Oversized)
			require.Equal(t, row.WebURL, spokeRows.Results[i].WebURL)
		}
	}
	require.True(t, oversizedFound)
	result, err = session.CallTool(ctx, &sdkmcp.CallToolParams{Name: "kata.search", Arguments: map[string]any{"query": "concept", "project": project.Name, "mode": "semantic", "labels": []string{"oversized"}}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	output, err = json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.Contains(t, string(output), `"oversized":true`)

	// This issue is created after the index fill, so only the lexical leg can
	// find it. Its local copy remains open and unlabeled after the hub changes it.
	late, _, err := hub.DB.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID, Title: "lexical needle", Body: "old body", Author: "tester"})
	require.NoError(t, err)
	events, err = hub.DB.EventsAfter(ctx, db.EventsAfterParams{ProjectID: project.ID, IssueUID: late.UID, Types: []string{"issue.created"}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, events, 1)
	e = events[0]
	_, err = spoke.DB.InsertRemoteEvent(ctx, replica.ID, db.RemoteEvent{EventUID: e.UID, OriginInstanceUID: e.OriginInstanceUID, ProjectUID: e.ProjectUID, ProjectName: e.ProjectName, IssueUID: e.IssueUID, RelatedIssueUID: e.RelatedIssueUID, Type: e.Type, Actor: e.Actor, HLCPhysicalMS: e.HLCPhysicalMS, HLCCounter: e.HLCCounter, ContentHash: e.ContentHash, Payload: jsontext.Value(e.Payload), CreatedAt: e.CreatedAt})
	require.NoError(t, err)
	require.NoError(t, spoke.DB.MaterializeFederatedProject(ctx, replica.ID))
	lateLocal, err := spoke.DB.IssueByUID(ctx, late.UID, db.IncludeDeletedNo)
	require.NoError(t, err)
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: late.ID, Title: new("current needle title"), Body: new("current body"), Actor: "tester"})
	require.NoError(t, err)
	_, _, _, err = hub.DB.CloseIssue(ctx, late.ID, "done", "tester", "", nil)
	require.NoError(t, err)
	_, err = hub.DB.AddLabel(ctx, late.ID, "current-label", "tester")
	require.NoError(t, err)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=needle&status=closed&label=current-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Len(t, body.Results, 1)
	require.Equal(t, lateLocal.ID, body.Results[0].Issue.ID)
	require.Equal(t, "current needle title", body.Results[0].Issue.Title)
	require.Equal(t, "current body", body.Results[0].Issue.Body)
	require.Equal(t, "closed", body.Results[0].Issue.Status)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=needle&exclude_label=current-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	for _, hit := range body.Results {
		require.NotEqual(t, late.UID, hit.Issue.UID)
	}
	_, _, _, err = hub.DB.EditIssue(ctx, db.EditIssueParams{IssueID: late.ID, Title: new("current title"), Actor: "tester"})
	require.NoError(t, err)
	resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=needle&status=closed&label=current-label", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	require.NoError(t, json.Unmarshal(raw, &body))
	for _, hit := range body.Results {
		require.NotEqual(t, late.UID, hit.Issue.UID, "a stale local word match must not survive a hub text edit")
	}
}

func TestDirectSpokeSearchReportsUnavailableHub(t *testing.T) {
	hub, spoke, hubProject, replica, _, token := createClaimForwardingPair(t, "pull")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{
		HubURL: hub.URL, HubProjectID: hubProject.ID, Token: token, Actor: "tester", Capabilities: "pull",
	}))
	setClaimBindingHubURL(t, spoke, replica.ID, "http://127.0.0.1:1")
	path := fmt.Sprintf("/api/v1/projects/%d/search?q=claim", replica.ID)
	resp, body := envGetRaw(t, spoke, path+"&mode=semantic")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(body))
	resp, body = envGetRaw(t, spoke, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"degraded":true`)
	// Preparing a native leave persists a credential marker and fences the binding.
	credentials := config.DefaultFederationCredentialStore()
	_, err := daemon.PrepareFederationReplicaLeave(t.Context(), spoke.DB, credentials, replica.ID, "search-test")
	require.NoError(t, err)
	binding, err := spoke.DB.FederationBindingByProject(t.Context(), replica.ID)
	require.NoError(t, err)
	require.True(t, binding.Enabled)
	require.False(t, binding.PushEnabled)
	credential, found, err := credentials.FederationCredential(t.Context(), replica.UID)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, credential.NativeLeave)
	resp, body = envGetRaw(t, spoke, path+"&mode=semantic")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(body))
	resp, body = envGetRaw(t, spoke, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"degraded":true`)
	binding.Enabled = false
	_, err = spoke.DB.UpsertFederationBinding(t.Context(), binding)
	require.NoError(t, err)
	resp, body = envGetRaw(t, spoke, path+"&mode=semantic")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(body))
	resp, body = envGetRaw(t, spoke, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"degraded":true`)
}

func TestFederationSearchRequiresPullEnrollment(t *testing.T) {
	hub, spoke, project, _, _, token := createClaimForwardingPair(t, "pull")
	path := fmt.Sprintf("/api/v1/projects/%d/federation/search?q=claim", project.ID)
	resp, body := envDoRaw(t, hub, http.MethodGet, path, nil, map[string]string{"Authorization": "Bearer " + token})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
	resp, body = envDoRaw(t, hub, http.MethodGet, path, nil, map[string]string{"Authorization": "Bearer invalid-token"})
	require.Equal(t, http.StatusForbidden, resp.StatusCode, string(body))
	push := createClaimEnrollment(t, hub, project.ID, spoke.DB.InstanceUID(), "push")
	resp, body = envDoRaw(t, hub, http.MethodGet, path, nil, map[string]string{"Authorization": "Bearer " + push.Token})
	require.Equal(t, http.StatusForbidden, resp.StatusCode, string(body))
	other := createFederatedHubProject(t, hub, "other-project")
	otherPath := fmt.Sprintf("/api/v1/projects/%d/federation/search?q=claim", other.ID)
	resp, body = envDoRaw(t, hub, http.MethodGet, otherPath, nil, map[string]string{"Authorization": "Bearer " + token})
	require.Equal(t, http.StatusForbidden, resp.StatusCode, string(body))
	parent, _, err := hub.DB.CreateAPIToken(t.Context(), db.CreateAPITokenParams{Actor: "tester", AdminActor: "admin", PlaintextToken: "relay-parent-search-token"})
	require.NoError(t, err)
	public, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, hub.DB.PinRootAuthority(t.Context(), db.RootKeyPin{ProjectUID: project.UID, AuthorityUID: hub.DB.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}))
	relay, err := hub.DB.CreateRelayEnrollment(t.Context(), db.CreateRelayEnrollmentParams{ProjectID: project.ID, ParentTokenID: parent.ID, SpokeInstanceUID: spoke.DB.InstanceUID(), ProtocolVersion: db.RelayProtocolVersion, Token: "relay-search-token"})
	require.NoError(t, err)
	resp, body = envDoRaw(t, hub, http.MethodGet, path, nil, map[string]string{"Authorization": "Bearer " + relay.Token})
	require.Equal(t, http.StatusForbidden, resp.StatusCode, string(body))
	require.Contains(t, string(body), "relay_protocol_required")
}

func TestDirectSpokeSearchUnsupportedHub(t *testing.T) {
	hub, spoke, project, replica, _, token := createClaimForwardingPair(t, "pull")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: project.ID, Token: token}))
	resp, body := envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=claim&mode=semantic", replica.ID))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(body))
	require.Contains(t, string(body), "requires [search.embeddings]")
}

func TestDirectSpokeSearchOlderHub(t *testing.T) {
	hub, spoke, project, replica, _, token := createClaimForwardingPair(t, "pull")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: project.ID, Token: token}))
	older := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(older.Close)
	setClaimBindingHubURL(t, spoke, replica.ID, older.URL)
	path := fmt.Sprintf("/api/v1/projects/%d/search?q=claim", replica.ID)
	for _, mode := range []string{"semantic", "hybrid"} {
		resp, body := envGetRaw(t, spoke, path+"&mode="+mode)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(body))
	}
	resp, body := envGetRaw(t, spoke, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"mode":"lexical"`)
	require.Contains(t, string(body), `"degraded":true`)
	require.Contains(t, string(body), "claim target")
}

func TestDirectSpokeSearchSlowHub(t *testing.T) {
	hub, spoke, project, replica, _, token := createClaimForwardingPair(t, "pull")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: project.ID, Token: token}))
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(slow.Close)
	setClaimBindingHubURL(t, spoke, replica.ID, slow.URL)
	type response struct {
		mode   string
		status int
		body   string
		err    error
	}
	results := make(chan response, 2)
	for _, mode := range []string{"auto", "semantic"} {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			path := fmt.Sprintf("%s/api/v1/projects/%d/search?q=claim&mode=%s", spoke.URL, replica.ID, mode)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
			if err != nil {
				results <- response{mode: mode, err: err}
				return
			}
			resp, err := spoke.HTTP.Do(req)
			if err != nil {
				results <- response{mode: mode, err: err}
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			results <- response{mode: mode, status: resp.StatusCode, body: string(body), err: err}
		}()
	}
	for range 2 {
		got := <-results
		require.NoError(t, got.err, got.mode)
		if got.mode == "auto" {
			require.Equal(t, http.StatusOK, got.status, got.body)
			require.Contains(t, got.body, `"degraded":true`)
			require.Contains(t, got.body, "claim target")
		} else {
			require.Equal(t, http.StatusServiceUnavailable, got.status, got.body)
		}
	}
}

func TestDirectSpokeSearchProjectAccessPrecedesHub(t *testing.T) {
	t.Setenv("KATA_HOME", t.TempDir())
	store, err := testfix.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "kata.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	f := newProjectAccessFixture(t, store)
	var calls atomic.Int32
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(hub.Close)
	_, err = store.UpsertFederationBinding(t.Context(), db.FederationBinding{ProjectID: f.private.ID, Role: db.FederationRoleSpoke, HubURL: hub.URL, HubProjectID: 1, HubProjectUID: f.private.UID, Actor: "member", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, config.WriteFederationCredential(f.private.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: 1, Token: "pull-test-token"}))
	path := fmt.Sprintf("/api/v1/projects/%d/search?q=claim&mode=semantic", f.private.ID)
	status, _, body := f.request(t, http.MethodGet, path, "nonmember", nil, nil)
	require.Equal(t, http.StatusNotFound, status, string(body))
	require.Zero(t, calls.Load())
	status, _, body = f.request(t, http.MethodGet, path, "member", nil, nil)
	require.Equal(t, http.StatusServiceUnavailable, status, string(body))
	require.EqualValues(t, 1, calls.Load())
}

func TestDirectSpokeSearchReadonlyRequiresAuthentication(t *testing.T) {
	env := testenv.New(t, testenv.WithInsecureReadonly())
	project, err := env.DB.CreateProject(t.Context(), "shared-project")
	require.NoError(t, err)
	_, _, err = env.DB.CreateIssue(t.Context(), db.CreateIssueParams{ProjectID: project.ID, Title: "claim target", Author: "tester"})
	require.NoError(t, err)
	_, err = env.DB.UpsertFederationBinding(t.Context(), db.FederationBinding{ProjectID: project.ID, Role: db.FederationRoleSpoke, HubURL: "https://hub.example", HubProjectID: 1, HubProjectUID: project.UID, Enabled: true})
	require.NoError(t, err)
	path := fmt.Sprintf("/api/v1/projects/%d/search?q=claim", project.ID)
	resp, body := envGetRaw(t, env, path+"&mode=semantic")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, string(body))
	resp, body = envGetRaw(t, env, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"mode":"lexical"`)
}
