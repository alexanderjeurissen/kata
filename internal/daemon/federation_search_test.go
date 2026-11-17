package daemon_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/config"
	"go.kenn.io/kata/internal/daemon"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/embedding"
	mcpserver "go.kenn.io/kata/internal/mcp"
	"go.kenn.io/kata/internal/testenv"
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
	hub := testenv.New(t, func(cfg *daemon.ServerConfig) { cfg.Embedder, cfg.VectorIndex = emb, idx })
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
	_, err = spoke.DB.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: replica.ID, Role: db.FederationRoleSpoke, HubURL: hub.URL, HubProjectID: project.ID, HubProjectUID: project.UID, Actor: "tester", Enabled: true})
	require.NoError(t, err)
	events, err := hub.DB.EventsAfter(ctx, db.EventsAfterParams{ProjectID: project.ID, IssueUID: issue.UID, Types: []string{"issue.created"}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, events, 1)
	e := events[0]
	_, err = spoke.DB.InsertRemoteEvent(ctx, replica.ID, db.RemoteEvent{EventUID: e.UID, OriginInstanceUID: e.OriginInstanceUID, ProjectUID: e.ProjectUID, ProjectName: e.ProjectName, IssueUID: e.IssueUID, RelatedIssueUID: e.RelatedIssueUID, Type: e.Type, Actor: e.Actor, HLCPhysicalMS: e.HLCPhysicalMS, HLCCounter: e.HLCCounter, ContentHash: e.ContentHash, Payload: jsontext.Value(e.Payload), CreatedAt: e.CreatedAt})
	require.NoError(t, err)
	require.NoError(t, spoke.DB.MaterializeFederatedProject(ctx, replica.ID))
	grant := createClaimEnrollment(t, hub, project.ID, spoke.DB.InstanceUID(), "pull")
	require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: hub.URL, HubProjectID: project.ID, Token: grant.Token}))
	filteredPath := fmt.Sprintf("/api/v1/projects/%d/federation/search?q=concept&label=missing", project.ID)
	filteredResp, filtered := envDoRaw(t, hub, http.MethodGet, filteredPath, nil, map[string]string{"Authorization": "Bearer " + grant.Token})
	require.Equal(t, http.StatusOK, filteredResp.StatusCode, string(filtered))
	require.Contains(t, string(filtered), `"results":[]`)
	resp, raw := envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=concept&mode=semantic", replica.ID))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	var body struct {
		Results []struct {
			Issue db.Issue `json:"issue"`
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
