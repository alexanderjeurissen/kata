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
