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
	"go.kenn.io/kata/internal/uid"
)

// RunRelayMovePeerReset checks peer access to a retained move checkpoint.
func RunRelayMovePeerReset(t *testing.T, store db.Storage) {
	ctx := t.Context()
	var projects [3]db.Project
	for i, name := range []string{"move-source", "move-destination", "move-peer"} {
		var err error
		projects[i], err = store.CreateProject(ctx, name)
		require.NoError(t, err)
		_, err = store.UpsertFederationBinding(ctx, db.FederationBinding{ProjectID: projects[i].ID, Role: db.FederationRoleHub, HubProjectID: projects[i].ID, HubProjectUID: projects[i].UID, Enabled: true})
		require.NoError(t, err)
	}
	peer, _, err := store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projects[2].ID, Author: "member", Title: "Peer"})
	require.NoError(t, err)
	comment, _, err := store.CreateComment(ctx, db.CreateCommentParams{IssueID: peer.ID, Author: "member", Body: "Peer discussion"})
	require.NoError(t, err)
	issue, _, err := store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projects[0].ID, Author: "member", Title: "Moving issue", Metadata: map[string]jsontext.Value{notification.MetadataKey("recipient"): jsontext.Value(`{"re":"` + comment.UID + `"}`)}})
	require.NoError(t, err)
	moved, err := store.MoveIssueProject(ctx, db.MoveIssueProjectIn{IssueID: issue.ID, FromProjectID: projects[0].ID, ToProjectID: projects[1].ID, IfMatchRev: issue.Revision, Actor: "member"})
	require.NoError(t, err)
	require.NotNil(t, moved.Event)
	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pin := db.RootKeyPin{ProjectUID: projects[1].UID, AuthorityUID: store.InstanceUID(), KeyID: db.RootPublicKeyID(public), PublicKey: public}
	require.NoError(t, store.PinRootAuthority(ctx, pin))
	parentToken, err := uid.New()
	require.NoError(t, err)
	parent, _, err := store.CreateAPIToken(ctx, db.CreateAPITokenParams{Actor: "member", AdminActor: "admin", PlaintextToken: parentToken})
	require.NoError(t, err)
	grant, err := store.CreateRelayEnrollment(ctx, db.CreateRelayEnrollmentParams{ProjectID: projects[1].ID, ParentTokenID: parent.ID, SpokeInstanceUID: "00000000000000000000000006", ProtocolVersion: db.RelayProtocolVersion})
	require.NoError(t, err)
	binding := grant.Enrollment.RelayBindingUID
	signer := db.RootAttributionSigner{AuthorityUID: store.InstanceUID(), PrivateKey: private}
	ctx = db.WithRelayPeerEventFeatures(ctx, db.SupportedEventFeatures)
	reset := store.(db.RelayResetStore)
	checkpoint, err := reset.CreateRelayReset(ctx, binding, signer)
	require.NoError(t, err)
	require.NoError(t, db.VerifyRootResetManifest(pin, checkpoint.Manifest, checkpoint.Snapshot))
	events, _, err := db.DecodeRootResetPayload(pin, checkpoint.Snapshot)
	require.NoError(t, err)
	var found bool
	for _, event := range events {
		if event.EventUID == moved.Event.UID {
			found = true
			require.Equal(t, "issue.moved", event.Type)
			require.Equal(t, moved.Event.ContentHash, event.ContentHash)
		}
	}
	require.True(t, found)
	denied := errors.New("peer denied")
	rejecting := db.WithProjectAccessTransactionCheck(ctx, func(_ context.Context, _ db.Transaction, uid string) error {
		if uid == projects[2].UID {
			return denied
		}
		return nil
	})
	before := relayStateForBinding(t, store, binding)
	blocked, err := reset.CreateRelayReset(rejecting, binding, signer)
	require.ErrorIs(t, err, denied)
	require.Equal(t, db.RelayResetCheckpoint{}, blocked)
	require.Equal(t, before, relayStateForBinding(t, store, binding))
	retry, err := reset.CreateRelayReset(ctx, binding, signer)
	require.NoError(t, err)
	require.Equal(t, checkpoint, retry)
}
