package dbtest

import (
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

// RunRelayCrossLinkResetHistory checks reset feature gating and forwarded peer-denial rollback.
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
	_, _, err = root.CreateLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), params, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	spokeUID := "00000000000000000000000008"
	unlink := newRemoteEvent(t, projects[0], &issues[0].UID, "issue.unlinked", "member", spokeUID, time.Now().Add(time.Second).UnixMilli(), jsontext.Value(`{"issue_uid":"`+issues[0].UID+`","from_uid":"`+issues[0].UID+`","to_uid":"`+issues[1].UID+`","link_from_uid":"`+issues[0].UID+`","link_to_uid":"`+issues[1].UID+`","type":"blocks"}`))
	unlink.RelatedIssueUID = &issues[1].UID
	unlink.ContentHash = remoteEventHash(t, unlink)
	_, err = root.IngestFederationEvents(ctx, db.FederationIngestParams{ProjectID: projects[0].ID, SpokeInstanceUID: spokeUID, Events: []db.FederationIngestEvent{{SourceEventID: 1, Event: unlink}}})
	require.NoError(t, err)
	require.Empty(t, linkTargetUIDs(t, root, issues[0].ID))
	peerBinding := enroll(root, hub, projects[1], "reset-peer-hub", true)
	binding := enroll(root, hub, projects[0], "reset-hub", true)
	require.NoError(t, root.MaterializeFederatedProject(ctx, projects[0].ID))
	require.Empty(t, linkTargetUIDs(t, root, issues[0].ID), "enrollment preserves the direct unlink")
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
	features := db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	denied := errors.New("peer denied")
	rejectPeer := db.WithProjectAccessTransactionCheck(features, func(_ context.Context, _ db.Transaction, uid string) error {
		if uid == projects[1].UID {
			return denied
		}
		return nil
	})
	deliver(t)
	hubIssue, err := hub.IssueByUID(ctx, issues[0].UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	require.Empty(t, linkTargetUIDs(t, hub, hubIssue.ID), "forwarded events preserve the direct unlink")
	require.Empty(t, linkTargetUIDs(t, root, issues[0].ID))
	_, _, err = root.CreateLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateLinkParams{FromIssueID: issues[0].ID, ToIssueID: issues[1].ID, Type: "parent", Author: "root-member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: issues[0].ID, Actor: "root-member", FromUID: issues[0].UID, ToUID: issues[1].UID, FromShortID: issues[0].ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	rootResets := root.(db.RelayResetStore)
	relayState := func(store db.Storage) []db.ImportRecord {
		var records []db.ImportRecord
		for record, err := range store.ExportRelayState(ctx) {
			require.NoError(t, err)
			records = append(records, record)
		}
		return records
	}
	projectEvents := func(store db.Storage, projectID int64) []db.EventExport {
		var events []db.EventExport
		for event, err := range store.ExportEvents(ctx, db.ExportFilter{ProjectID: &projectID, IncludeDeleted: true}) {
			require.NoError(t, err)
			events = append(events, event)
		}
		return events
	}
	checkpointMeta := func(store db.Storage, projectUID string) []db.MetaKV {
		var metadata []db.MetaKV
		for record, err := range store.ExportMeta(ctx) {
			require.NoError(t, err)
			if strings.HasPrefix(record.Key, db.RelayResetMetadataPrefix+projectUID) {
				metadata = append(metadata, record)
			}
		}
		return metadata
	}
	before := relayState(root)
	eventsBefore := projectEvents(root, projects[0].ID)
	metaBefore := checkpointMeta(root, projects[0].UID)
	grants, err := root.ListProjectFederationEnrollments(ctx, projects[0].ID)
	require.NoError(t, err)
	_, err = rootResets.CreateRelayReset(ctx, binding, signer)
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	require.Equal(t, before, relayState(root))
	require.Equal(t, eventsBefore, projectEvents(root, projects[0].ID))
	require.Equal(t, metaBefore, checkpointMeta(root, projects[0].UID))
	afterGrants, err := root.ListProjectFederationEnrollments(ctx, projects[0].ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	blocked, err := rootResets.CreateRelayReset(rejectPeer, binding, signer)
	require.ErrorIs(t, err, denied)
	require.Equal(t, db.RelayResetCheckpoint{}, blocked)
	require.Equal(t, before, relayState(root))
	require.Equal(t, eventsBefore, projectEvents(root, projects[0].ID))
	require.Equal(t, metaBefore, checkpointMeta(root, projects[0].UID))
	afterGrants, err = root.ListProjectFederationEnrollments(ctx, projects[0].ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	checkpoint, err := rootResets.CreateRelayReset(features, binding, signer)
	require.NoError(t, err)
	public := signer.PrivateKey.Public().(ed25519.PublicKey)
	resetEvents, _, err := db.DecodeRootResetPayload(db.RootKeyPin{ProjectUID: projects[0].UID, AuthorityUID: root.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}, checkpoint.Snapshot)
	require.NoError(t, err)
	require.Contains(t, resetEvents, unlink, "the checkpoint retains the direct unlink")
	require.NotEqual(t, eventsBefore, projectEvents(root, projects[0].ID))
	require.NotEqual(t, metaBefore, checkpointMeta(root, projects[0].UID))

	retainedState := relayState(root)
	retainedEvents := projectEvents(root, projects[0].ID)
	retainedMeta := checkpointMeta(root, projects[0].UID)
	blocked, err = rootResets.CreateRelayReset(rejectPeer, binding, signer)
	require.ErrorIs(t, err, denied)
	require.Equal(t, db.RelayResetCheckpoint{}, blocked)
	require.Equal(t, retainedState, relayState(root))
	require.Equal(t, retainedEvents, projectEvents(root, projects[0].ID))
	require.Equal(t, retainedMeta, checkpointMeta(root, projects[0].UID))
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
	// The signed reset retains the root-authored parent link.
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, hub, hubIssue.ID))
	hubProject, err := hub.ProjectByUID(ctx, projects[0].UID)
	require.NoError(t, err)
	downstream := enroll(hub, leaf, hubProject, "reset-leaf", false)
	drain := func(binding string) {
		for _, stream := range []string{db.RelayStreamEvent, db.RelayStreamReceipt, db.RelayStreamArtifact} {
			for {
				items, err := hub.PendingRelayDeliveries(features, binding, stream, 100)
				require.NoError(t, err)
				if len(items) == 0 {
					break
				}
				last := items[len(items)-1]
				require.NoError(t, hub.AckRelayDeliveries(ctx, binding, 1, stream, last.Sequence, last.Digest))
			}
		}
	}
	drain(downstream)
	before = relayState(hub)
	eventsBefore = projectEvents(hub, hubProject.ID)
	metaBefore = checkpointMeta(hub, hubProject.UID)
	grants, err = hub.ListProjectFederationEnrollments(ctx, hubProject.ID)
	require.NoError(t, err)
	_, err = hubResets.CreateRelayReset(ctx, downstream, db.RootAttributionSigner{})
	require.ErrorIs(t, err, db.ErrUnsupportedEventFeatures)
	require.Equal(t, before, relayState(hub))
	require.Equal(t, eventsBefore, projectEvents(hub, hubProject.ID))
	require.Equal(t, metaBefore, checkpointMeta(hub, hubProject.UID))
	afterGrants, err = hub.ListProjectFederationEnrollments(ctx, hubProject.ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	before = relayState(hub)
	eventsBefore = projectEvents(hub, hubProject.ID)
	metaBefore = checkpointMeta(hub, hubProject.UID)
	_, err = hubResets.CreateRelayReset(rejectPeer, downstream, db.RootAttributionSigner{})
	require.ErrorIs(t, err, denied)
	require.Equal(t, before, relayState(hub))
	require.Equal(t, eventsBefore, projectEvents(hub, hubProject.ID))
	require.Equal(t, metaBefore, checkpointMeta(hub, hubProject.UID))
	afterGrants, err = hub.ListProjectFederationEnrollments(ctx, hubProject.ID)
	require.NoError(t, err)
	require.Equal(t, grants, afterGrants)
	forwarded, err := hubResets.CreateRelayReset(features, downstream, db.RootAttributionSigner{})
	require.NoError(t, err)
	require.NotEqual(t, metaBefore, checkpointMeta(hub, hubProject.UID))
	require.Equal(t, checkpoint.Manifest, forwarded.Manifest)
	require.Equal(t, checkpoint.Snapshot, forwarded.Snapshot)
	peerProject, err := hub.ProjectByUID(ctx, projects[1].UID)
	require.NoError(t, err)
	peerDownstream := enroll(hub, leaf, peerProject, "reset-peer-leaf", false)
	drain(peerDownstream)
	peerForwarded, err := hubResets.CreateRelayReset(features, peerDownstream, db.RootAttributionSigner{})
	require.NoError(t, err)
	leafResets := leaf.(db.RelayResetStore)
	require.NoError(t, leafResets.InstallRelayReset(ctx, peerDownstream, peerForwarded.Manifest, peerForwarded.Snapshot, peerForwarded.Translation))
	require.NoError(t, leafResets.InstallRelayReset(ctx, downstream, forwarded.Manifest, forwarded.Snapshot, forwarded.Translation))
	leafIssue, err := leaf.IssueByUID(ctx, issues[0].UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	require.Equal(t, []string{issues[1].UID}, linkTargetUIDs(t, leaf, leafIssue.ID))
	// A relay that has only received the first project must withhold its
	// checkpoint until the linked issue arrives with the second project.
	partial := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, partial.Close()) })
	partialLeaf := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, partialLeaf.Close()) })
	partialBinding := enroll(root, partial, projects[0], "partial-reset-hub", true)
	partialCheckpoint, err := rootResets.CreateRelayReset(features, partialBinding, signer)
	require.NoError(t, err)
	partialResets := partial.(db.RelayResetStore)
	require.NoError(t, partialResets.InstallRelayReset(ctx, partialBinding, partialCheckpoint.Manifest, partialCheckpoint.Snapshot, partialCheckpoint.Translation))
	partialProject, err := partial.ProjectByUID(ctx, projects[0].UID)
	require.NoError(t, err)
	partialDownstream := enroll(partial, partialLeaf, partialProject, "partial-reset-leaf", false)
	partialState := relayState(partial)
	partialMeta := checkpointMeta(partial, partialProject.UID)
	missingReset, err := partialResets.CreateRelayReset(features, partialDownstream, db.RootAttributionSigner{})
	require.ErrorIs(t, err, db.ErrRelayLinkUnresolved)
	require.Equal(t, db.RelayResetCheckpoint{}, missingReset)
	require.Equal(t, partialState, relayState(partial))
	require.Equal(t, partialMeta, checkpointMeta(partial, partialProject.UID))
	partialPeer := enroll(root, partial, projects[1], "partial-reset-peer", true)
	partialPeerCheckpoint, err := rootResets.CreateRelayReset(features, partialPeer, signer)
	require.NoError(t, err)
	require.NoError(t, partialResets.InstallRelayReset(ctx, partialPeer, partialPeerCheckpoint.Manifest, partialPeerCheckpoint.Snapshot, partialPeerCheckpoint.Translation))
	partialRetry, err := partialResets.CreateRelayReset(features, partialDownstream, db.RootAttributionSigner{})
	require.NoError(t, err)
	require.Equal(t, partialCheckpoint.Manifest, partialRetry.Manifest)
	require.Equal(t, partialCheckpoint.Snapshot, partialRetry.Snapshot)
	archivedReplica := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, archivedReplica.Close()) })
	archivedBinding := enroll(root, archivedReplica, projects[0], "archive-source", true)
	archivedPeerBinding := enroll(root, archivedReplica, projects[1], "archive-peer", false)
	for _, current := range []string{archivedPeerBinding, archivedBinding} {
		items, err := root.PendingRelayDeliveries(features, current, db.RelayStreamEvent, 100)
		require.NoError(t, err)
		_, err = archivedReplica.AcceptRelayDeliveries(ctx, current, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: items})
		require.NoError(t, err)
		last := items[len(items)-1]
		require.NoError(t, root.AckRelayDeliveries(ctx, current, 1, db.RelayStreamEvent, last.Sequence, last.Digest))
	}
	archivedLeaf := backend.Open(t)
	t.Cleanup(func() { require.NoError(t, archivedLeaf.Close()) })
	archivedSource, err := archivedReplica.ProjectByUID(ctx, projects[0].UID)
	require.NoError(t, err)
	archivedDownstream := enroll(archivedReplica, archivedLeaf, archivedSource, "archive-leaf", false)
	for {
		items, err := archivedReplica.PendingRelayDeliveries(features, archivedDownstream, db.RelayStreamEvent, 100)
		require.NoError(t, err)
		if len(items) == 0 {
			break
		}
		last := items[len(items)-1]
		require.NoError(t, archivedReplica.AckRelayDeliveries(ctx, archivedDownstream, 1, db.RelayStreamEvent, last.Sequence, last.Digest))
	}
	archivedPeer, err := archivedReplica.ProjectByUID(ctx, projects[1].UID)
	require.NoError(t, err)
	_, _, err = archivedReplica.RemoveProject(ctx, db.RemoveProjectParams{ProjectID: archivedPeer.ID, Actor: "member", Force: true})
	require.NoError(t, err)
	newIssue, _, err := root.CreateIssue(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateIssueParams{ProjectID: projects[0].ID, Author: "root-member", Title: "After archive"})
	require.NoError(t, err)
	_, linkEvent, err := root.CreateLinkAndEvent(db.WithRootAttribution(ctx, signer, "root-member"), db.CreateLinkParams{FromIssueID: newIssue.ID, ToIssueID: issues[1].ID, Type: "blocks", Author: "root-member"}, db.LinkEventParams{EventType: "issue.linked", EventIssueID: newIssue.ID, Actor: "root-member", FromUID: newIssue.UID, ToUID: issues[1].UID, FromShortID: newIssue.ShortID, ToShortID: issues[1].ShortID})
	require.NoError(t, err)
	items, err := root.PendingRelayDeliveries(features, archivedBinding, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	_, err = archivedReplica.AcceptRelayDeliveries(ctx, archivedBinding, db.RelayBatch{Stream: db.RelayStreamEvent, Envelopes: items})
	require.NoError(t, err)
	_, err = archivedReplica.IssueByUID(ctx, newIssue.UID, db.IncludeDeletedYes)
	require.NoError(t, err)
	archivedDenied := errors.New("archived peer denied")
	checkedFeatures := db.WithProjectAccessTransactionCheck(features, func(ctx context.Context, tx db.Transaction, uid string) error {
		project, err := archivedReplica.ProjectByUIDTx(ctx, tx, uid)
		if errors.Is(err, db.ErrNotFound) || (err == nil && project.DeletedAt != nil) {
			return archivedDenied
		}
		return err
	})
	items, err = archivedReplica.PendingRelayDeliveries(checkedFeatures, archivedDownstream, db.RelayStreamEvent, 100)
	require.NoError(t, err)
	require.NotEmpty(t, items)
	require.Equal(t, linkEvent.UID, items[len(items)-1].SourceUID)
}
