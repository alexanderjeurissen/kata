package federation_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

func TestRelayCrossProjectParentReplacement(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			root := newRelayMatrixNode(t, backend, "root-member")
			relay := newRelayMatrixNode(t, backend, "relay-member")
			replicas := []*relayMatrixNode{
				{store: relay.store, http: relay.http, signer: relay.signer, userToken: relay.userToken, account: relay.account, credentials: relay.credentials},
				{store: relay.store, http: relay.http, signer: relay.signer, userToken: relay.userToken, account: relay.account, credentials: relay.credentials},
			}
			var projects [2]db.Project
			for i, name := range []string{"child-project", "parent-project"} {
				project, err := root.store.CreateProject(ctx, name)
				require.NoError(t, err)
				projects[i] = project
				_, err = root.store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: project.ID, Role: db.FederationRoleHub, HubProjectID: project.ID, HubProjectUID: project.UID, Enabled: true})
				require.NoError(t, err)
				root.project = project
				enrollRelayMatrixReplica(t, root, replicas[i], name+"-relay", true)
				syncRelayMatrixNode(t, replicas[i])
			}
			rootCtx := db.WithRootAttribution(ctx, root.signer, root.account)
			child, _, err := root.store.CreateIssue(rootCtx, db.CreateIssueParams{ProjectID: projects[0].ID, Title: "Child", Author: root.account})
			require.NoError(t, err)
			oldParent, _, err := root.store.CreateIssue(rootCtx, db.CreateIssueParams{ProjectID: projects[1].ID, Title: "Old parent", Author: root.account})
			require.NoError(t, err)
			newParent, _, err := root.store.CreateIssue(rootCtx, db.CreateIssueParams{ProjectID: projects[0].ID, Title: "New parent", Author: root.account})
			require.NoError(t, err)
			linkEvent := func(eventType string, parent db.Issue) db.LinkEventParams {
				return db.LinkEventParams{EventType: eventType, EventIssueID: child.ID, Actor: root.account, FromUID: child.UID, ToUID: parent.UID, FromShortID: child.ShortID, ToShortID: parent.ShortID}
			}
			oldLink, _, err := root.store.CreateLinkAndEvent(rootCtx, db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: oldParent.ID, Type: "parent", Author: root.account}, linkEvent("issue.linked", oldParent))
			require.NoError(t, err)
			for _, replica := range replicas {
				syncRelayMatrixNode(t, replica)
			}
			localChildID := issueIDForRelayMatrix(t, replicas[0], child.UID)
			parent, err := relay.store.ParentOf(ctx, localChildID)
			require.NoError(t, err)
			require.Equal(t, oldParent.UID, parent.ToIssueUID)

			_, err = root.store.ReplaceParentAndEvents(rootCtx, db.ReplaceParentAndEventsParams{
				ExpectedParentLinkID: oldLink.ID, ExpectedParentIssueID: oldParent.ID,
				Link:        db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: newParent.ID, Type: "parent", Author: root.account},
				UnlinkEvent: linkEvent("issue.unlinked", oldParent), LinkEvent: linkEvent("issue.linked", newParent),
			})
			require.NoError(t, err)
			for _, replica := range replicas {
				syncRelayMatrixNode(t, replica)
			}
			parent, err = relay.store.ParentOf(ctx, localChildID)
			require.NoError(t, err)
			require.Equal(t, newParent.UID, parent.ToIssueUID)
			links, err := relay.store.LinksByIssue(ctx, localChildID)
			require.NoError(t, err)
			require.Len(t, links, 1)
		})
	}
}
