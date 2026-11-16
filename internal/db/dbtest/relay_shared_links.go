package dbtest

import (
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/notification"
)

// RelaySharedAccessFixture is one native root and its common-audience pair.
type RelaySharedAccessFixture struct {
	Projects [2]db.Project
	Issues   [2]db.Issue
	Grant    db.CreatedFederationEnrollment
	Team     db.Team
	Signer   db.RootAttributionSigner
}

// RunRelayEventReleasePeerAccess checks access before offering events and moved issue snapshots.
func RunRelayEventReleasePeerAccess(t *testing.T, store db.Storage) {
	ctx := t.Context()
	fixture := NewRelaySharedAccessFixture(t, store)
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	denied := errors.New("peer denied")
	rejectPeer := db.WithProjectAccessTransactionCheck(features, func(_ context.Context, _ db.Transaction, uid string) error {
		if uid == fixture.Projects[1].UID {
			return denied
		}
		return nil
	})
	binding := fixture.Grant.Enrollment.RelayBindingUID
	before := relayStateForBinding(t, store, binding)
	out, err := store.PendingRelayDeliveries(rejectPeer, binding, db.RelayStreamEvent, 100)
	require.ErrorIs(t, err, denied)
	require.Empty(t, out)
	require.Equal(t, before, relayStateForBinding(t, store, binding))
	offered, err := store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.NotEmpty(t, offered)
	out, err = store.PendingRelayDeliveries(rejectPeer, binding, db.RelayStreamEvent, 100)
	require.ErrorIs(t, err, denied)
	require.Empty(t, out)
	retry, err := store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.Equal(t, offered, retry)
	last := offered[len(offered)-1]
	require.NoError(t, store.AckRelayDeliveries(ctx, binding, 1, db.RelayStreamEvent, last.Sequence, last.Digest))
	_, ordinary, err := store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: fixture.Projects[0].ID, Author: "member", Title: "Before crossing"})
	require.NoError(t, err)
	link, crossing, err := store.CreateLinkAndEvent(db.WithRootAttribution(ctx, fixture.Signer, "member"), db.CreateLinkParams{FromIssueID: fixture.Issues[0].ID, ToIssueID: fixture.Issues[1].ID, Type: "related", Author: "member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: fixture.Issues[0].ID, Actor: "member", FromUID: fixture.Issues[0].UID, ToUID: fixture.Issues[1].UID})
	require.NoError(t, err)
	prefix, err := store.PendingRelayDeliveries(ctx, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.Len(t, prefix, 1)
	require.Equal(t, ordinary.UID, prefix[0].SourceUID)
	replayed, err := store.PendingRelayDeliveries(ctx, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.Equal(t, prefix, replayed)
	require.NoError(t, store.AckRelayDeliveries(ctx, binding, 1, db.RelayStreamEvent, prefix[0].Sequence, prefix[0].Digest))
	_, err = store.PendingRelayDeliveries(ctx, binding, db.RelayStreamEvent, 100)
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	next, err := store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, crossing.UID, next[0].SourceUID)
	require.Equal(t, crossing.ContentHash, next[0].SourceHash)
	require.NoError(t, store.AckRelayDeliveries(ctx, binding, 1, db.RelayStreamEvent, next[0].Sequence, next[0].Digest))
	unlinked, err := store.DeleteLinkAndEvent(db.WithRootAttribution(ctx, fixture.Signer, "member"), link, db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: fixture.Issues[0].ID, Actor: "member", FromUID: fixture.Issues[0].UID, ToUID: fixture.Issues[1].UID})
	require.NoError(t, err)
	_, err = store.PendingRelayDeliveries(ctx, binding, db.RelayStreamEvent, 100)
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	next, err = store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, unlinked.UID, next[0].SourceUID)
	require.NoError(t, store.RevokeFederationEnrollment(ctx, fixture.Grant.Enrollment.ID))
	source, err := store.CreateProject(ctx, "move-source")
	require.NoError(t, err)
	_, err = store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: source.ID, Role: db.FederationRoleHub, HubProjectID: source.ID, HubProjectUID: source.UID, Enabled: true})
	require.NoError(t, err)
	comment, _, err := store.CreateComment(ctx, db.CreateCommentParams{IssueID: fixture.Issues[1].ID, Author: "member", Body: "Peer comment"})
	require.NoError(t, err)
	issue, _, err := store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: fixture.Projects[0].ID, Author: "member", Title: "Moved issue"})
	require.NoError(t, err)
	patched, err := store.PatchIssueMetadata(ctx, db.PatchIssueMetadataIn{IssueID: issue.ID, Actor: "member", Patch: map[string]jsontext.Value{notification.MetadataKey("recipient"): jsontext.Value(`{"re":"` + comment.UID + `"}`)}})
	require.NoError(t, err)
	away, err := store.MoveIssueProject(ctx, db.MoveIssueProjectIn{IssueID: issue.ID, FromProjectID: fixture.Projects[0].ID, ToProjectID: source.ID, IfMatchRev: patched.Issue.Revision, Actor: "member"})
	require.NoError(t, err)
	moved, err := store.MoveIssueProject(ctx, db.MoveIssueProjectIn{IssueID: issue.ID, FromProjectID: source.ID, ToProjectID: fixture.Projects[0].ID, IfMatchRev: away.Issue.Revision, Actor: "member"})
	require.NoError(t, err)
	require.NotNil(t, moved.Event)
	require.Contains(t, moved.Event.Payload, comment.UID)
	grant, err := store.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: fixture.Projects[0].ID, ParentTokenID: *fixture.Grant.Enrollment.ParentTokenID, SpokeInstanceUID: "00000000000000000000000007", ProtocolVersion: db.RelayProtocolVersion, Token: "move-snapshot-relay-test-token"})
	require.NoError(t, err)
	binding = grant.Enrollment.RelayBindingUID
	for {
		items, err := store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NoError(t, store.AckRelayDeliveries(ctx, binding, 1, db.RelayStreamEvent, items[0].Sequence, items[0].Digest))
		if items[0].SourceUID == patched.Event.UID {
			break
		}
	}
	before = relayStateForBinding(t, store, binding)
	out, err = store.PendingRelayDeliveries(rejectPeer, binding, db.RelayStreamEvent, 100)
	require.ErrorIs(t, err, denied)
	require.Empty(t, out)
	require.Equal(t, before, relayStateForBinding(t, store, binding))
	offered, err = store.PendingRelayDeliveries(features, binding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.NotEmpty(t, offered)
	require.Equal(t, moved.Event.UID, offered[len(offered)-1].SourceUID)
}

// NewRelaySharedAccessFixture enrolls the same native pair for storage and
// relay tests, keeping the initial regression at native admission.
func NewRelaySharedAccessFixture(t *testing.T, store db.Storage) RelaySharedAccessFixture {
	t.Helper()

	ctx := t.Context()
	team, _, err := store.CreateTeam(ctx, "shared-audience", "admin")
	require.NoError(t, err)
	parent, _, err := store.CreateAPIToken(ctx, db.CreateAPITokenParams{
		Actor: "member", AdminActor: "admin", TeamUIDs: []string{team.UID},
		PlaintextToken: "shared-audience-parent-test-token",
	})
	require.NoError(t, err)
	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	var projects [2]db.Project
	var issues [2]db.Issue
	for i, name := range []string{"first-shared-project", "second-shared-project"} {
		projects[i], err = store.CreateProject(ctx, name)
		require.NoError(t, err)
		project := projects[i]
		policy, err := store.ProjectAccessPolicy(ctx, project.UID)
		require.NoError(t, err)
		policy.Visibility, policy.TeamUIDs = "teams", []string{team.UID}
		_, _, err = store.SetProjectAccessPolicy(ctx, policy, "admin")
		require.NoError(t, err)
		_, err = store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: project.ID,
			Role: db.FederationRoleHub, HubProjectID: project.ID, HubProjectUID: project.UID, Enabled: true})
		require.NoError(t, err)
		require.NoError(t, store.PinRootAuthority(ctx, db.RootKeyPin{ProjectUID: project.UID,
			AuthorityUID: store.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}))
		issues[i], _, err = store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: project.ID,
			Author: "member", Title: "Shared endpoint"})
		require.NoError(t, err)
	}
	_, _, err = store.CreateLinkAndEvent(ctx, db.CreateLinkParams{FromIssueID: issues[0].ID,
		ToIssueID: issues[1].ID, Type: "blocks", Author: "member"},
		db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "member",
			FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	grant, err := store.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: projects[0].ID,
		ParentTokenID: parent.ID, SpokeInstanceUID: "00000000000000000000000006",
		ProtocolVersion: db.RelayProtocolVersion, Token: "first-shared-relay-test-token"})
	require.NoError(t, err)

	return RelaySharedAccessFixture{Projects: projects, Issues: issues, Grant: grant, Team: team, Signer: db.RootAttributionSigner{AuthorityUID: store.InstanceUID(), PrivateKey: private}}
}
