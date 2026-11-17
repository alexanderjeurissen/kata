package dbtest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

func checkRelayArchivedSourceRestoration(t *testing.T, root db.Storage, backend Backend) error {
	ctx := t.Context()
	fixture := NewRelaySharedAccessFixture(t, root)
	replica := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, replica.Close()) })
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	peerBinding, peerEpoch := setupRelayHop(t, root, replica, fixture.Projects[1], "archive-peer", false)
	sourceBinding, sourceEpoch := setupRelayHop(t, root, replica, fixture.Projects[0], "archive-source", false)
	syncRelayHop(features, t, root, replica, peerBinding, peerEpoch, db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact)
	syncRelayHop(features, t, root, replica, sourceBinding, sourceEpoch, db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact)
	source, err := replica.ProjectByUID(ctx, fixture.Projects[0].UID)
	require.NoError(t, err)
	query := db.UISnapshotQuery{SelectedIssueUID: fixture.Issues[0].UID}
	ui := replica.(db.UIStore)
	snapshot, err := ui.ReadUISnapshot(ctx, query)
	require.NoError(t, err)
	require.Len(t, snapshot.SelectedLinks, 1)
	_, _, err = replica.RemoveProject(ctx, db.RemoveProjectParams{ProjectID: source.ID, Actor: "member", Force: true})
	require.NoError(t, err)
	_, _, err = root.CreateIssue(db.WithRootAttribution(ctx, fixture.Signer, "member"), db.CreateIssueParams{ProjectID: fixture.Projects[1].ID, Author: "member", Title: "Peer update"})
	require.NoError(t, err)
	syncRelayHop(features, t, root, replica, peerBinding, peerEpoch, db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact)
	_, _, changed, err := replica.RestoreProject(ctx, source.ID, "member")
	require.NoError(t, err)
	require.True(t, changed)
	snapshot, err = ui.ReadUISnapshot(ctx, query)
	require.NoError(t, err)
	require.Len(t, snapshot.SelectedLinks, 1)
	require.Equal(t, fixture.Issues[1].UID, snapshot.SelectedLinks[0].ToIssueUID)
	return nil
}

func checkRelayArchivedPeerReconnect(t *testing.T, store db.Storage) error {
	ctx := t.Context()
	fixture := NewRelaySharedAccessFixture(t, store)
	grant := fixture.Grant
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	syncRelayHop(features, t, store, nil, grant.Enrollment.RelayBindingUID, grant.Enrollment.RelayResetEpoch, db.RelayStreamEvent)
	before := relayStateForBinding(t, store, grant.Enrollment.RelayBindingUID)
	_, _, err := store.RemoveProject(ctx, db.RemoveProjectParams{ProjectID: fixture.Projects[1].ID, Actor: "member", Force: true})
	require.NoError(t, err)
	_, _, err = store.RevokeAPIToken(ctx, *grant.Enrollment.ParentTokenID, "admin")
	require.NoError(t, err)
	parent, _, err := store.CreateAPIToken(ctx, db.CreateAPITokenParams{Actor: "member", AdminActor: "admin", TeamUIDs: []string{fixture.Team.UID}, PlaintextToken: "replacement-parent-test-token"})
	require.NoError(t, err)
	t.Run("reconnect check", func(t *testing.T) {
		require.NoError(t, store.CheckRelayProjectLinks(ctx, fixture.Projects[0].ID))
	})
	t.Run("credential rebind", func(t *testing.T) {
		rebound, err := store.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: fixture.Projects[0].ID, ParentTokenID: parent.ID, SpokeInstanceUID: grant.Enrollment.SpokeInstanceUID, ProtocolVersion: db.RelayProtocolVersion, Token: grant.Token, RebindParent: true})
		require.NoError(t, err)
		require.Equal(t, grant.Enrollment.RelayBindingUID, rebound.Enrollment.RelayBindingUID)
		require.Equal(t, grant.Enrollment.RelayResetEpoch, rebound.Enrollment.RelayResetEpoch)
		require.Equal(t, before, relayStateForBinding(t, store, grant.Enrollment.RelayBindingUID))
		_, err = store.AuthorizeFederationToken(ctx, grant.Token, fixture.Projects[0].ID, "pull")
		require.NoError(t, err)
	})
	return nil
}
