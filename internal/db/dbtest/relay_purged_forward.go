package dbtest

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/uid"
)

// RunRelayPurgedForward checks that a refreshed root checkpoint can cross two
// relay hops after the root purges a linked issue.
func RunRelayPurgedForward(t *testing.T, backend Backend) {
	ctx := t.Context()
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	root := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	hub := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, hub.Close()) })
	leaf := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, leaf.Close()) })

	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer := db.RootAttributionSigner{AuthorityUID: root.InstanceUID(), PrivateKey: private}
	var projects [2]db.Project
	var issues [2]db.Issue
	for i, name := range []string{"purge-source", "purge-peer"} {
		projects[i], err = root.CreateProject(ctx, name)
		require.NoError(t, err)
		policy, err := root.ProjectAccessPolicy(ctx, projects[i].UID)
		require.NoError(t, err)
		policy.Visibility, policy.TeamUIDs = "all", nil
		_, _, err = root.SetProjectAccessPolicy(ctx, policy, "admin")
		require.NoError(t, err)
		_, err = root.UpsertFederationBinding(ctx, db.FederationBinding{
			ProjectID: projects[i].ID, Role: db.FederationRoleHub,
			HubProjectID: projects[i].ID, HubProjectUID: projects[i].UID, Enabled: true,
		})
		require.NoError(t, err)
		require.NoError(t, root.PinRootAuthority(ctx, db.RootKeyPin{
			ProjectUID: projects[i].UID, AuthorityUID: root.InstanceUID(),
			KeyID: db.RootPublicKeyID(public), PublicKey: public,
		}))
		issues[i], _, err = root.CreateIssue(ctx, db.CreateIssueParams{
			ProjectID: projects[i].ID, Author: "member", Title: name,
		})
		require.NoError(t, err)
	}
	_, linkEvent, err := root.CreateLinkAndEvent(
		db.WithRootAttribution(ctx, signer, "member"),
		db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "parent", Author: "member"},
		db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID},
	)
	require.NoError(t, err)
	cacheParent, _, err := root.CreateAPIToken(ctx, db.CreateAPITokenParams{
		Actor: "member", AdminActor: "admin", PlaintextToken: "cache-refresh-parent-test-token",
	})
	require.NoError(t, err)
	cacheSpokeUID, err := uid.New()
	require.NoError(t, err)
	cacheGrant, err := root.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{
		ProjectID: projects[0].ID, ParentTokenID: cacheParent.ID, SpokeInstanceUID: cacheSpokeUID,
		ProtocolVersion: db.RelayProtocolVersion, Token: "cache-refresh-relay-" + cacheSpokeUID,
	})
	require.NoError(t, err)
	rootResets := root.(db.RelayResetStore)
	cached, err := rootResets.CreateRelayReset(features, cacheGrant.Enrollment.RelayBindingUID, signer)
	require.NoError(t, err)
	rootPin, err := root.RootAuthority(ctx, projects[0].UID)
	require.NoError(t, err)
	cachedEvents, _, err := db.DecodeRootResetPayload(rootPin, cached.Snapshot)
	require.NoError(t, err)
	var cachedLink bool
	for _, event := range cachedEvents {
		if event.EventUID == linkEvent.UID {
			cachedLink = true
			require.Equal(t, linkEvent.ContentHash, event.ContentHash)
		}
	}
	require.True(t, cachedLink)

	_, err = root.PurgeIssue(ctx, issues[1].ID, "admin", nil)
	require.NoError(t, err)
	retainedLink, err := root.EventsByUIDs(ctx, projects[0].ID, []string{linkEvent.UID})
	require.NoError(t, err)
	require.Len(t, retainedLink, 1)
	require.Equal(t, linkEvent.ID, retainedLink[0].ID)
	require.Nil(t, retainedLink[0].RelatedIssueID, "purge detaches the backend-local endpoint reference")
	require.Equal(t, linkEvent.ContentHash, retainedLink[0].ContentHash)
	require.Equal(t, db.RemoteEventFromStored(linkEvent), db.RemoteEventFromStored(retainedLink[0]), "purge preserves the signed source event")
	refreshedCache, err := rootResets.CreateRelayReset(features, cacheGrant.Enrollment.RelayBindingUID, signer)
	require.NoError(t, err)
	require.NotEqual(t, cached.Manifest.SnapshotUID, refreshedCache.Manifest.SnapshotUID)
	refreshedCacheEvents, _, err := db.DecodeRootResetPayload(rootPin, refreshedCache.Snapshot)
	require.NoError(t, err)
	for _, event := range refreshedCacheEvents {
		require.NotEqual(t, linkEvent.UID, event.EventUID, "a purged endpoint cannot remain in retained link history")
	}

	rootBinding, rootEpoch := setupRelayHop(t, root, hub, projects[0], "purge-hub", true)
	syncRelayHop(features, t, root, hub, rootBinding, rootEpoch, db.RelayStreamEvent)
	refreshed, err := rootResets.CreateRelayReset(features, rootBinding, signer)
	require.NoError(t, err)
	refreshedEvents, _, err := db.DecodeRootResetPayload(rootPin, refreshed.Snapshot)
	require.NoError(t, err)
	for _, event := range refreshedEvents {
		require.NotEqual(t, linkEvent.UID, event.EventUID, "the newly enrolled intermediate receives the filtered checkpoint")
	}
	require.NoError(t, hub.(db.RelayResetStore).InstallRelayReset(ctx, rootBinding, refreshed.Manifest, refreshed.Snapshot, refreshed.Translation))

	hubProject, err := hub.ProjectByUID(ctx, projects[0].UID)
	require.NoError(t, err)
	leafBinding, leafEpoch := setupRelayHop(t, hub, leaf, hubProject, "purge-leaf", false)
	syncRelayHop(features, t, hub, nil, leafBinding, leafEpoch, db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact)
	forwarded, err := hub.(db.RelayResetStore).CreateRelayReset(features, leafBinding, db.RootAttributionSigner{})
	require.NoError(t, err)
	require.Equal(t, refreshed.Manifest, forwarded.Manifest)
	require.Equal(t, refreshed.Snapshot, forwarded.Snapshot, "the intermediate forwards the root's signed bytes unchanged")
	require.NoError(t, leaf.(db.RelayResetStore).InstallRelayReset(ctx, leafBinding, forwarded.Manifest, forwarded.Snapshot, forwarded.Translation))

	later, laterEvent, err := root.CreateIssue(ctx, db.CreateIssueParams{
		ProjectID: projects[0].ID, Author: "member", Title: "After purge",
	})
	require.NoError(t, err)
	items, err := root.PendingRelayDeliveries(
		db.WithRelayRequestedEpoch(features, refreshed.Translation.Authority.Epoch), rootBinding, db.RelayStreamEvent, 100,
	)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, laterEvent.UID, items[0].SourceUID)
	_, err = hub.AcceptRelayDeliveries(ctx, rootBinding, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: items})
	require.NoError(t, err)
	require.NoError(t, root.AckRelayDeliveries(ctx, rootBinding, refreshed.Translation.Authority.Epoch, db.RelayStreamEvent, items[0].Sequence, items[0].Digest))

	items, err = hub.PendingRelayDeliveries(
		db.WithRelayRequestedEpoch(features, forwarded.Translation.Authority.Epoch), leafBinding, db.RelayStreamEvent, 100,
	)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, laterEvent.UID, items[0].SourceUID)
	_, err = leaf.AcceptRelayDeliveries(ctx, leafBinding, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: items})
	require.NoError(t, err)
	require.NoError(t, hub.AckRelayDeliveries(ctx, leafBinding, forwarded.Translation.Authority.Epoch, db.RelayStreamEvent, items[0].Sequence, items[0].Digest))

	for _, replica := range []db.Storage{hub, leaf} {
		got, err := replica.IssueByUID(ctx, later.UID, db.IncludeDeletedYes)
		require.NoError(t, err)
		require.Equal(t, "After purge", got.Title)
	}
}
