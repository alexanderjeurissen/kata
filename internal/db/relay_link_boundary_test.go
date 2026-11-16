package db_test

import (
	"context"
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

func TestLegacyRelayAdmissionRejectsCrossingRemovals(t *testing.T) {
	const local = "00000000000000000000000001"
	const foreign = "00000000000000000000000002"
	crosses := func(_ context.Context, uid string) (bool, error) { return uid == foreign, nil }
	noCrossing := func(context.Context, string) (bool, error) { return false, nil }
	for _, tc := range []struct {
		name, kind, payload string
		rejected            bool
	}{
		{"unlink crossing", "issue.unlinked", `{"from_uid":"` + local + `","to_uid":"` + foreign + `"}`, true},
		{"unlink local", "issue.unlinked", `{"from_uid":"` + local + `","to_uid":"` + local + `"}`, false},
		{"mixed blocks", "issue.links_changed", `{"blocks_added_uids":["` + local + `"],"blocks_removed_uids":["` + foreign + `"]}`, true},
		{"blocked by", "issue.links_changed", `{"blocked_by_removed_uids":["` + foreign + `"]}`, true},
		{"related", "issue.links_changed", `{"related_removed_uids":["` + foreign + `"]}`, true},
		{"local removal", "issue.links_changed", `{"related_removed_uids":["` + local + `"]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := db.ValidateLegacyRelayAdmission(t.Context(), db.RemoteEvent{Type: tc.kind, Payload: jsontext.Value(tc.payload)}, crosses, noCrossing, noCrossing)
			if tc.rejected {
				require.ErrorIs(t, err, db.ErrFederationIngestValidation)
				require.ErrorIs(t, err, db.ErrFederationCrossProjectBoundary)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// A relay-sourced issue.links_changed folds without its crossing additions and
// with every removal, crossing or not, over links the project already had.
func TestStripCrossProjectLinkAdditionsFoldsMixedLinksChanged(t *testing.T) {
	const (
		project       = "00000000000000000000000001"
		base          = "00000000000000000000000010"
		sameOld       = "00000000000000000000000011"
		sameBlocks    = "00000000000000000000000012"
		sameBlockedBy = "00000000000000000000000013"
		sameRelated   = "00000000000000000000000014"
		foreignOld    = "00000000000000000000000020"
		foreignParent = "00000000000000000000000021"
		foreignNew    = "00000000000000000000000022"
		foreignBlocks = "00000000000000000000000023"
		foreignBlkBy  = "00000000000000000000000024"
		foreignRel    = "00000000000000000000000025"
	)
	foreign := map[string]bool{foreignOld: true, foreignParent: true, foreignNew: true, foreignBlocks: true, foreignBlkBy: true, foreignRel: true}
	crossProject := func(peer string) bool { return foreign[peer] }
	event := func(uid, eventType string, at int64, payload string) db.FoldEvent {
		return db.FoldEvent{UID: uid, OriginInstanceUID: "00000000000000000000000099", ProjectUID: project, IssueUID: base, Type: eventType, Actor: "relay-member", HLCPhysicalMS: at, CreatedAt: "2026-05-23T12:00:00.000Z", Payload: jsontext.Value(payload)}
	}
	// Links the project already had: a same-project edge, and crossing edges a
	// legacy peer created before relay enrollment.
	history := []db.FoldEvent{
		event("00000000000000000000000031", "issue.linked", 100, `{"issue_uid":"`+base+`","from_uid":"`+base+`","to_uid":"`+sameOld+`","type":"blocks"}`),
		event("00000000000000000000000032", "issue.linked", 101, `{"issue_uid":"`+base+`","from_uid":"`+base+`","to_uid":"`+foreignOld+`","type":"blocks"}`),
		event("00000000000000000000000033", "issue.linked", 102, `{"issue_uid":"`+base+`","from_uid":"`+base+`","to_uid":"`+foreignParent+`","type":"parent"}`),
	}
	changed := event("00000000000000000000000034", "issue.links_changed", 200, `{"issue_uid":"`+base+`",`+
		`"parent_set_uid":"`+foreignNew+`","parent_removed_uid":"`+foreignParent+`",`+
		`"blocks_added_uids":["`+sameBlocks+`","`+foreignBlocks+`"],"blocks_removed_uids":["`+sameOld+`","`+foreignOld+`"],`+
		`"blocked_by_added_uids":["`+sameBlockedBy+`","`+foreignBlkBy+`"],`+
		`"related_added_uids":["`+sameRelated+`","`+foreignRel+`"]}`)

	stripped, keep, err := db.StripCrossProjectLinkAdditions(changed, db.RelayLinkPolicy{CrossProject: crossProject, SameRoot: func(string) bool { return false }})
	require.NoError(t, err)
	require.True(t, keep, "a links_changed with same-project effects stays in the fold")
	projection := db.FoldEvents(append(history, stripped))

	present := map[db.FoldLinkKey]bool{}
	for key, state := range projection.Links {
		present[key] = state.Present
	}
	require.Equal(t, map[db.FoldLinkKey]bool{
		{FromUID: base, ToUID: sameOld, Type: "blocks"}:       false,
		{FromUID: base, ToUID: foreignOld, Type: "blocks"}:    false,
		{FromUID: base, ToUID: foreignParent, Type: "parent"}: false,
		{FromUID: base, ToUID: sameBlocks, Type: "blocks"}:    true,
		{FromUID: sameBlockedBy, ToUID: base, Type: "blocks"}: true,
		{FromUID: base, ToUID: sameRelated, Type: "related"}:  true,
	}, present, "crossing additions are stripped; same-project additions and all removals are applied")
}
