package dbtest

import (
	"crypto/ed25519"
	"encoding/json/jsontext"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/uid"
)

// RunRelayCrossLinkResetHistory checks feature gating for cached and forwarded reset history.
func RunRelayCrossLinkResetHistory(t *testing.T, root db.Storage, backend Backend) {
	ctx := t.Context()
	hub := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, hub.Close()) })
	leaf := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, leaf.Close()) })
	_, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer := db.RootAttributionSigner{AuthorityUID: root.InstanceUID(), PrivateKey: private}
	enroll := func(upstream, replica db.Storage, project db.Project, name string, downstream bool) string {
		t.Helper()
		parent, _, err := upstream.CreateAPIToken(ctx, db.CreateAPITokenParams{Actor: "member", AdminActor: "admin", PlaintextToken: name + "-parent-test-token"})
		require.NoError(t, err)
		grant, err := upstream.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: project.ID, ParentTokenID: parent.ID, SpokeInstanceUID: replica.InstanceUID(), ProtocolVersion: db.RelayProtocolVersion, ServeDownstream: downstream, Token: name + "-relay-test-token"})
		require.NoError(t, err)
		local, err := replica.CreateProjectWithUID(ctx, name, project.UID)
		require.NoError(t, err)
		pin, err := upstream.RootAuthority(ctx, project.UID)
		require.NoError(t, err)
		require.NoError(t, replica.PinRootAuthority(ctx, pin))
		_, err = replica.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: local.ID, Role: db.FederationRoleSpoke, HubURL: "https://hub.example", HubProjectID: project.ID, HubProjectUID: project.UID, Actor: "member", Enabled: true, PushEnabled: true})
		require.NoError(t, err)
		path := []string{pin.AuthorityUID, replica.InstanceUID()}
		if upstream.InstanceUID() != pin.AuthorityUID {
			path = []string{pin.AuthorityUID, upstream.InstanceUID(), replica.InstanceUID()}
		}
		_, err = replica.SetRelayBindingConfig(ctx, local.ID, db.RelayBindingConfig{ProtocolVersion: db.RelayProtocolVersion, BindingUID: grant.Enrollment.RelayBindingUID, AuthorityUID: pin.AuthorityUID, UpstreamInstanceUID: upstream.InstanceUID(), HubPath: path, LocalActor: "member", ServeDownstream: downstream, ResetEpoch: 1})
		require.NoError(t, err)
		return grant.Enrollment.RelayBindingUID
	}
	var projects [2]db.Project
	var issues [2]db.Issue
	for i, name := range []string{"reset-source", "reset-peer"} {
		project, err := root.CreateProject(ctx, name)
		require.NoError(t, err)
		projects[i] = project
		_, err = root.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: project.ID, Role: db.FederationRoleHub, HubProjectID: project.ID, HubProjectUID: project.UID, Enabled: true})
		require.NoError(t, err)
		public := signer.PrivateKey.Public().(ed25519.PublicKey)
		require.NoError(t, root.PinRootAuthority(ctx, db.RootKeyPin{ProjectUID: project.UID, AuthorityUID: root.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}))
		issues[i], _, err = root.CreateIssue(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateIssueParams{ProjectID: project.ID, Author: "root-member", Title: name})
		require.NoError(t, err)
	}
	params := db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "blocks", Author: "root-member"}
	blocksLink, _, err := root.CreateLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), params, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	localParent, _, err := root.CreateIssue(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateIssueParams{ProjectID: projects[0].ID, Author: "root-member", Title: "Local parent"})
	require.NoError(t, err)
	_, _, err = root.CreateLinkAndEvent(ctx, db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: localParent.ID, Type: "related", Author: "member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, FromUID: issues[0].UID, ToUID: localParent.UID})
	require.NoError(t, err)
	peerBinding := enroll(root, hub, projects[1], "reset-peer-hub", true)
	spoke, err := uid.New()
	require.NoError(t, err)
	removed := newRemoteEvent(t, projects[0], &issues[0].UID, "issue.links_changed", "member", spoke, time.Now().UnixMilli()+10, jsontext.Value(`{"blocks_removed_uids":["`+issues[1].UID+`"],"related_removed_uids":["`+localParent.UID+`"]}`))
	_, err = root.IngestFederationEvents(db.WithAuthorizedProjects(ctx, []string{projects[0].UID, projects[1].UID}), db.FederationIngestParams{ProjectID: projects[0].ID, SpokeInstanceUID: spoke, Events: []db.FederationIngestEvent{{SourceEventID: 1, Event: removed}}})
	require.NoError(t, err)
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, root, issues[0].ID))
	binding := enroll(root, hub, projects[0], "reset-hub", true)
	deliver := func(t *testing.T) {
		t.Helper()
		features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
		for _, current := range []string{peerBinding, binding} {
			for {
				items, err := root.PendingRelayDeliveries(features, current, db.RelayStreamEvent, 100)
				require.NoError(t, err)
				if len(items) == 0 {
					break
				}
				_, err = hub.AcceptRelayDeliveries(ctx, current, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: items})
				require.NoError(t, err)
				last := items[len(items)-1]
				require.NoError(t, root.AckRelayDeliveries(ctx, current, 1, db.RelayStreamEvent, last.Sequence, last.Digest))
			}
		}
	}
	deliver(t)
	hubIssue, err := hub.IssueByUID(ctx, issues[0].UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, hub, hubIssue.ID))
	_, err = root.DeleteLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), blocksLink, db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID})
	require.NoError(t, err)
	require.Empty(t, linkTargetUIDs(t, root, issues[0].ID))
	parentLink, _, err := root.CreateLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "parent", Author: "root-member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	t.Run("replacement reaches relay", func(t *testing.T) {
		child, _, err := root.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projects[0].ID, Author: "member", Title: "Replacement child"})
		require.NoError(t, err)
		old, _, err := root.CreateLinkAndEvent(ctx, db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: localParent.ID, Type: "parent", Author: "member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: child.ID, FromUID: child.UID, ToUID: localParent.UID})
		require.NoError(t, err)
		replacement, err := root.(db.ParentLinkReplacementStorage).ReplaceParentAndEvents(ctx, db.ReplaceParentAndEventsParams{
			ExpectedParentLinkID: old.ID, ExpectedParentIssueID: localParent.ID,
			Link:        db.CreateLinkParams{FromIssueID: child.ID, ToIssueID: issues[1].ID, Type: "parent", Author: "member"},
			UnlinkEvent: db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: child.ID, FromUID: child.UID, ToUID: localParent.UID},
			LinkEvent:   db.LinkEventParams{EventType: "issue.linked", EventIssueID: child.ID, FromUID: child.UID, ToUID: issues[1].UID},
		})
		require.NoError(t, err)
		deliver(t)
		hubChild, err := hub.IssueByUID(ctx, child.UID, db.IncludeDeletedYes)
		require.NoError(t, err)
		require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, hub, hubChild.ID))
		_, err = root.DeleteLinkAndEvent(ctx, replacement.Link, db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: child.ID, Actor: "member", FromUID: child.UID, ToUID: issues[1].UID})
		require.NoError(t, err)
	})
	rootResets := root.(db.RelayResetStore)
	relayState := func(store db.Storage) []db.ImportRecord {
		var records []db.ImportRecord
		for record, err := range store.ExportRelayState(ctx) {
			require.NoError(t, err)
			records = append(records, record)
		}
		return records
	}
	before := relayState(root)
	grants, err := root.ListProjectFederationEnrollments(ctx, projects[0].ID)
	require.NoError(t, err)
	_, err = rootResets.CreateRelayReset(ctx, binding, signer)
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	require.Equal(t, before, relayState(root))
	afterGrants, err := root.ListProjectFederationEnrollments(ctx, projects[0].ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	checkpoint, err := rootResets.CreateRelayReset(features, binding, signer)
	require.NoError(t, err)

	_, err = rootResets.CreateRelayReset(ctx, binding, signer)
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	retry, err := rootResets.CreateRelayReset(features, binding, signer)
	require.NoError(t, err)
	require.Equal(t, checkpoint, retry)
	hubResets := hub.(db.RelayResetStore)
	require.NoError(t, hubResets.InstallRelayReset(ctx, binding, checkpoint.Manifest, checkpoint.Snapshot, checkpoint.Translation))
	peerCheckpoint, err := rootResets.CreateRelayReset(features, peerBinding, signer)
	require.NoError(t, err)
	require.NoError(t, hubResets.InstallRelayReset(ctx, peerBinding, peerCheckpoint.Manifest, peerCheckpoint.Snapshot, peerCheckpoint.Translation))
	hubIssue, err = hub.IssueByUID(ctx, issues[0].UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	// The signed reset retains the root-authored removal.
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, hub, hubIssue.ID))
	hubProject, err := hub.ProjectByUID(ctx, projects[0].UID)
	require.NoError(t, err)
	downstream := enroll(hub, leaf, hubProject, "reset-leaf", false)
	for _, stream := range []string{db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact} {
		for {
			items, err := hub.PendingRelayDeliveries(features, downstream, stream, 100)
			require.NoError(t, err)
			if len(items) == 0 {
				break
			}
			last := items[len(items)-1]
			require.NoError(t, hub.AckRelayDeliveries(ctx, downstream, 1, stream, last.Sequence, last.Digest))
		}
	}
	before = relayState(hub)
	grants, err = hub.ListProjectFederationEnrollments(ctx, hubProject.ID)
	require.NoError(t, err)
	_, err = hubResets.CreateRelayReset(ctx, downstream, db.RootAttributionSigner{})
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	require.Equal(t, before, relayState(hub))
	afterGrants, err = hub.ListProjectFederationEnrollments(ctx, hubProject.ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	forwarded, err := hubResets.CreateRelayReset(features, downstream, db.RootAttributionSigner{})
	require.NoError(t, err)
	require.Equal(t, checkpoint.Manifest, forwarded.Manifest)
	require.Equal(t, checkpoint.Snapshot, forwarded.Snapshot)
	peerProject, err := hub.ProjectByUID(ctx, projects[1].UID)
	require.NoError(t, err)
	peerDownstream := enroll(hub, leaf, peerProject, "reset-peer-leaf", false)
	for _, stream := range []string{db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact} {
		for {
			items, err := hub.PendingRelayDeliveries(features, peerDownstream, stream, 100)
			require.NoError(t, err)
			if len(items) == 0 {
				break
			}
			last := items[len(items)-1]
			require.NoError(t, hub.AckRelayDeliveries(ctx, peerDownstream, 1, stream, last.Sequence, last.Digest))
		}
	}
	peerForwarded, err := hubResets.CreateRelayReset(features, peerDownstream, db.RootAttributionSigner{})
	require.NoError(t, err)
	leafResets := leaf.(db.RelayResetStore)
	require.NoError(t, leafResets.InstallRelayReset(ctx, peerDownstream, peerForwarded.Manifest, peerForwarded.Snapshot, peerForwarded.Translation))
	require.NoError(t, leafResets.InstallRelayReset(ctx, downstream, forwarded.Manifest, forwarded.Snapshot, forwarded.Translation))
	leafIssue, err := leaf.IssueByUID(ctx, issues[0].UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, leaf, leafIssue.ID))
	t.Run("root parent replacement after peer leaves group", func(t *testing.T) {
		peerProject, err := hub.ProjectByUID(ctx, projects[1].UID)
		require.NoError(t, err)
		peerConfig, err := hub.FederationBindingByProject(ctx, peerProject.ID)
		require.NoError(t, err)
		peerConfig.Enabled = false
		_, err = hub.UpsertFederationBinding(ctx, peerConfig)
		require.NoError(t, err)
		event := newRemoteEvent(t, projects[0], &issues[0].UID, "issue.links_changed", "root-member", root.InstanceUID(), time.Now().UnixMilli()+100, jsontext.Value(`{"parent_removed_uid":"`+issues[1].UID+`","parent_set_uid":"`+localParent.UID+`"}`))
		body, err := db.EncodeRelaySourceEvent(event)
		require.NoError(t, err)
		sequence := checkpoint.Translation.HopBaselines.Events + 1
		envelope, err := db.SealRelayEnvelope(db.RelayEnvelope{Version: db.RelayProtocolVersion, BindingUID: binding, ProjectUID: projects[0].UID, AuthorityUID: root.InstanceUID(), SenderInstanceUID: root.InstanceUID(), ReceiverInstanceUID: hub.InstanceUID(), Epoch: checkpoint.Translation.Authority.Epoch, Sequence: sequence, Stream: db.RelayStreamEvent, Path: []string{root.InstanceUID()}, SourceUID: event.EventUID, SourceHash: event.ContentHash, Body: body})
		require.NoError(t, err)
		_, err = hub.AcceptRelayDeliveries(ctx, binding, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: []db.RelayEnvelope{envelope}})
		require.NoError(t, err)
		require.Equal(t, []string{localParent.UID}, linkTargetUIDs(t, hub, hubIssue.ID))
	})
	_, err = root.DeleteLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), parentLink, db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	t.Run("archive refreshes reset history", func(t *testing.T) {
		bootstrap := root.(db.RelayResetBootstrapStore)
		//nolint:gosec // This is a deterministic fixture credential.
		parent, _, err := root.CreateAPIToken(ctx, db.CreateAPITokenParams{Actor: "member", AdminActor: "admin", PlaintextToken: "archived-peer-parent-test-token"})
		require.NoError(t, err)
		//nolint:gosec // This is a deterministic fixture credential.
		warmGrant, err := root.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: projects[0].ID, ParentTokenID: parent.ID, SpokeInstanceUID: "00000000000000000000000005", ProtocolVersion: db.RelayProtocolVersion, Token: "warm-relay-test-token"})
		require.NoError(t, err)
		required, err := bootstrap.RelayEnrollmentNeedsReset(ctx, warmGrant.Enrollment.RelayBindingUID)
		require.NoError(t, err)
		require.False(t, required)
		_, _, err = root.RemoveProject(ctx, db.RemoveProjectParams{ProjectID: projects[1].ID, Actor: "root-member", Force: true})
		require.NoError(t, err)
		//nolint:gosec // This is a deterministic fixture credential.
		newGrant, err := root.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: projects[0].ID, ParentTokenID: parent.ID, SpokeInstanceUID: "00000000000000000000000006", ProtocolVersion: db.RelayProtocolVersion, Token: "archived-peer-relay-test-token"})
		require.NoError(t, err)
		required, err = bootstrap.RelayEnrollmentNeedsReset(ctx, newGrant.Enrollment.RelayBindingUID)
		require.NoError(t, err)
		require.True(t, required)
		newReset, err := rootResets.CreateRelayReset(features, newGrant.Enrollment.RelayBindingUID, signer)
		require.NoError(t, err)
		requireResetOmitsIssue(t, newReset, issues[1])
		afterArchive, err := rootResets.CreateRelayReset(features, binding, signer)
		require.NoError(t, err)
		requireResetOmitsIssue(t, afterArchive, issues[1])
		retry, err = rootResets.CreateRelayReset(features, binding, signer)
		require.NoError(t, err)
		require.Equal(t, afterArchive, retry)
	})
}

func RunRelayReplaceArchivedParent(t *testing.T, root db.Storage) {
	ctx := t.Context()
	_, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	var projects [2]db.Project
	var issues [2]db.Issue
	for i, name := range []string{"parent-source", "parent-peer"} {
		projects[i], err = root.CreateProject(ctx, name)
		require.NoError(t, err)
		_, err = root.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: projects[i].ID, Role: db.FederationRoleHub, HubProjectID: projects[i].ID, HubProjectUID: projects[i].UID, Enabled: true})
		require.NoError(t, err)
		public := private.Public().(ed25519.PublicKey)
		require.NoError(t, root.PinRootAuthority(ctx, db.RootKeyPin{ProjectUID: projects[i].UID, AuthorityUID: root.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}))
		issues[i], _, err = root.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projects[i].ID, Author: "member", Title: name})
		require.NoError(t, err)
	}
	parentToken, _, err := root.CreateAPIToken(ctx, db.CreateAPITokenParams{Actor: "member", AdminActor: "admin", PlaintextToken: "parent-replace-token"})
	require.NoError(t, err)
	_, err = root.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: projects[0].ID, ParentTokenID: parentToken.ID, SpokeInstanceUID: "00000000000000000000000007", ProtocolVersion: db.RelayProtocolVersion, Token: "parent-replace-relay-token"})
	require.NoError(t, err)
	newParent, _, err := root.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projects[0].ID, Author: "member", Title: "New parent"})
	require.NoError(t, err)
	old, _, err := root.CreateLinkAndEvent(ctx, db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "parent", Author: "member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, FromUID: issues[0].UID, ToUID: issues[1].UID})
	require.NoError(t, err)
	_, _, err = root.RemoveProject(ctx, db.RemoveProjectParams{ProjectID: projects[1].ID, Actor: "member", Force: true})
	require.NoError(t, err)
	_, err = root.(db.ParentLinkReplacementStorage).ReplaceParentAndEvents(ctx, db.ReplaceParentAndEventsParams{
		ExpectedParentLinkID: old.ID, ExpectedParentIssueID: issues[1].ID,
		Link:        db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: newParent.ID, Type: "parent", Author: "member"},
		UnlinkEvent: db.LinkEventParams{EventType: "issue.unlinked", EventIssueID: issues[0].ID},
		LinkEvent:   db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, FromUID: issues[0].UID, ToUID: newParent.UID},
	})
	require.NoError(t, err)
	parent, err := root.ParentOf(ctx, issues[0].ID)
	require.NoError(t, err)
	require.Equal(t, newParent.UID, parent.ToIssueUID)
}
