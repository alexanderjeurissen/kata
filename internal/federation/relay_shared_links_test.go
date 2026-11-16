package federation_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

// Use the existing real HTTP matrix nodes and transport, including both peer
// arrival orders at each hop; no special reference or projection fixture.
func TestRelaySharedLinksAcrossHops(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", backend, reverse), func(t *testing.T) {
				ctx := t.Context()
				root := newRelayMatrixNode(t, backend, "root-member")
				hub := newRelayMatrixNode(t, backend, "hub-member")
				leaf := newRelayMatrixNode(t, backend, "leaf-member")
				roots := []*relayMatrixNode{root, root}
				hubs := []*relayMatrixNode{{store: hub.store, http: hub.http, signer: hub.signer, userToken: hub.userToken, account: hub.account, credentials: hub.credentials},
					{store: hub.store, http: hub.http, signer: hub.signer, userToken: hub.userToken, account: hub.account, credentials: hub.credentials}}
				leaves := []*relayMatrixNode{{store: leaf.store, http: leaf.http, signer: leaf.signer, userToken: leaf.userToken, account: leaf.account, credentials: leaf.credentials},
					{store: leaf.store, http: leaf.http, signer: leaf.signer, userToken: leaf.userToken, account: leaf.account, credentials: leaf.credentials}}
				var projects [2]db.Project
				var issues [2]db.Issue
				for i, name := range []string{"first-shared", "second-shared"} {
					project, err := root.store.CreateProject(ctx, name)
					require.NoError(t, err)
					projects[i] = project
					_, err = root.store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: project.ID, Role: db.FederationRoleHub, HubProjectID: project.ID, HubProjectUID: project.UID, Enabled: true})
					require.NoError(t, err)
					root.project = project
					enrollRelayMatrixReplica(t, roots[i], hubs[i], name+"-hub", true)
					syncRelayMatrixNode(t, hubs[i])
					enrollRelayMatrixReplica(t, hubs[i], leaves[i], name+"-leaf", false)
					syncRelayMatrixNode(t, leaves[i])
					issues[i], _, err = root.store.CreateIssue(db.WithRootAttribution(ctx, root.signer, root.account), db.CreateIssueParams{ProjectID: project.ID, Author: "assistant", Title: "Shared endpoint"})
					require.NoError(t, err)
				}
				_, linkEvent, err := root.store.CreateLinkAndEvent(db.WithRootAttribution(ctx, root.signer, root.account), db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "blocks", Author: root.account},
					db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: root.account, FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
				require.NoError(t, err)
				order := []int{0, 1}
				if reverse {
					order = []int{1, 0}
				}
				for _, nodes := range [][]*relayMatrixNode{hubs, leaves} {

					for position, i := range order {
						syncRelayMatrixNode(t, nodes[i])
						if position == 0 && i == 0 {
							links, err := nodes[0].store.LinksByIssue(ctx, issueIDForRelayMatrix(t, nodes[0], issues[0].UID))
							require.NoError(t, err)
							require.Empty(t, links)
						}
					}
					_, err := nodes[1].store.IssueByUID(ctx, issues[1].UID, db.IncludeDeletedYes)
					require.NoError(t, err)
					stored, err := nodes[0].store.EventsByUIDs(ctx, nodes[0].project.ID, []string{linkEvent.UID})
					require.NoError(t, err)
					require.Len(t, stored, 1)
					require.Equal(t, linkEvent.ContentHash, stored[0].ContentHash)
					require.Equal(t, linkEvent.Payload, stored[0].Payload)
					links, err := nodes[0].store.LinksByIssue(ctx, issueIDForRelayMatrix(t, nodes[0], issues[0].UID))
					require.NoError(t, err)
					require.Len(t, links, 1)
					require.Equal(t, issues[1].UID, links[0].ToIssueUID)
				}
				links, err := hubs[0].store.LinksByIssue(ctx, issueIDForRelayMatrix(t, hubs[0], issues[0].UID))
				require.NoError(t, err)
				_, err = hubs[0].store.DeleteLinkAndEvent(ctx, links[0], db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: issueIDForRelayMatrix(t, hubs[0], issues[0].UID), Actor: hub.account, FromUID: issues[0].UID, ToUID: issues[1].UID})
				require.NoError(t, err)
				syncRelayMatrixNode(t, hubs[0])
				syncRelayMatrixNode(t, leaves[0])
				for _, node := range []*relayMatrixNode{root, leaves[0]} {
					links, err := node.store.LinksByIssue(ctx, issueIDForRelayMatrix(t, node, issues[0].UID))
					require.NoError(t, err)
					require.Empty(t, links)
				}
				if reverse {
					return
				}
				child, _, err := root.store.CreateIssue(db.WithRootAttribution(ctx, root.signer, root.account), db.CreateIssueParams{ProjectID: projects[0].ID, Author: root.account, Title: "Replacement child"})
				require.NoError(t, err)
				localParent, _, err := root.store.CreateIssue(db.WithRootAttribution(ctx, root.signer, root.account), db.CreateIssueParams{ProjectID: projects[0].ID, Author: root.account, Title: "Local parent"})
				require.NoError(t, err)
				old, _, err := root.store.CreateLinkAndEvent(db.WithRootAttribution(ctx, root.signer, root.account), db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: localParent.ID, Type: "parent", Author: root.account}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: child.ID, Actor: root.account, FromUID: child.UID, ToUID: localParent.UID})
				require.NoError(t, err)
				_, err = root.store.(db.ParentLinkReplacementStorage).ReplaceParentAndEvents(db.WithRootAttribution(ctx, root.signer, root.account), db.ReplaceParentAndEventsParams{
					ExpectedParentLinkID: old.ID, ExpectedParentIssueID: localParent.ID,
					Link:        db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: issues[1].ID, Type: "parent", Author: root.account},
					UnlinkEvent: db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: child.ID, Actor: root.account, FromUID: child.UID, ToUID: localParent.UID},
					LinkEvent:   db.LinkEventParams{EventType: "issue.linked", EventIssueID: child.ID, Actor: root.account, FromUID: child.UID, ToUID: issues[1].UID},
				})
				require.NoError(t, err)
				for _, nodes := range [][]*relayMatrixNode{hubs, leaves} {
					for _, i := range order {
						syncRelayMatrixNode(t, nodes[i])
					}
					parent, err := nodes[0].store.ParentOf(ctx, issueIDForRelayMatrix(t, nodes[0], child.UID))
					require.NoError(t, err)
					require.Equal(t, issues[1].UID, parent.ToIssueUID)
				}
			})
		}
	}
}

func TestRelaySharedLinksPreflightFeatures(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			root := newRelayMatrixNode(t, backend, "root-member")
			var projects [2]db.Project
			var issues [2]db.Issue
			for i, name := range []string{"preflight-source", "preflight-peer"} {
				project, err := root.store.CreateProject(ctx, name)
				require.NoError(t, err)
				projects[i] = project
				_, err = root.store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: project.ID, Role: db.FederationRoleHub, HubProjectID: project.ID, HubProjectUID: project.UID, Enabled: true})
				require.NoError(t, err)
				issues[i], _, err = root.store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID, Author: root.account, Title: name})
				require.NoError(t, err)
			}
			url := fmt.Sprintf("%s/api/v1/projects/%d/federation?relay_protocol_version=1", root.http.URL, projects[0].ID)
			get := func(features string) (int, string) {
				t.Helper()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer "+root.userToken)
				req.Header.Set(db.EventFeaturesHeader, features)
				resp, err := root.http.Client().Do(req)
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				return resp.StatusCode, string(body)
			}
			oldFeatures := db.CronEventFeature + "," + db.IssueMoveEventFeature + "," + db.RepeatedAdoptionEventFeature
			status, body := get(oldFeatures)
			require.Equal(t, http.StatusOK, status, body)
			link, _, err := root.store.CreateLinkAndEvent(ctx, db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "blocks", Author: root.account}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: root.account, FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
			require.NoError(t, err)
			root.project = projects[0]
			hub := newRelayMatrixNode(t, backend, "hub-member")
			enrollRelayMatrixReplica(t, root, hub, "preflight-hub", true)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/projects/%d/federation/relay/reset", root.http.URL, projects[0].ID), nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+hub.credential.Token)
			response, err := root.http.Client().Do(req)
			require.NoError(t, err)
			resetError, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusConflict, response.StatusCode, string(resetError))
			require.Contains(t, string(resetError), "unsupported_event_features")
			require.Contains(t, string(resetError), db.RelayCrossLinksEventFeature)
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/projects/%d/federation/relay/reset", root.http.URL, projects[0].ID), nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+hub.credential.Token)
			req.Header.Set(db.EventFeaturesHeader, db.SupportedEventFeatures)
			response, err = root.http.Client().Do(req)
			require.NoError(t, err)
			resetBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusOK, response.StatusCode, string(resetBody))
			_, err = root.store.DeleteLinkAndEvent(db.WithRootAttribution(ctx, root.signer, root.account), link, db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: issues[0].ID, Actor: root.account, FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
			require.NoError(t, err)
			status, body = get(oldFeatures)
			require.Equal(t, http.StatusOK, status, body)
			status, body = get(db.SupportedEventFeatures)
			require.Equal(t, http.StatusOK, status, body)
		})
	}
}
