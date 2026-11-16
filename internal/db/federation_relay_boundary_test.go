package db

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/notification"
)

func TestRelayEventPeerProjects(t *testing.T) {
	const issueUID = "00000000000000000000000001"
	const commentUID = "00000000000000000000000002"
	const ownIssueUID = "00000000000000000000000005"
	const otherIssueUID = "00000000000000000000000004"
	peer := Issue{ProjectUID: "peer"}
	related := issueUID
	ownIssue := ownIssueUID
	lookup := NewEventIssueLookup(func(uid string) (Issue, error) {
		if uid == issueUID {
			return peer, nil
		}
		if uid == ownIssueUID {
			return Issue{ProjectUID: "own"}, nil
		}
		if uid == otherIssueUID {
			return Issue{ProjectUID: "other"}, nil
		}
		return Issue{}, ErrNotFound
	}, func(_, ref string) (Issue, error) {
		if ref == issueUID {
			return peer, nil
		}
		return Issue{}, ErrNotFound
	}, func(uid string) (Issue, error) {
		if uid == commentUID {
			return peer, nil
		}
		return Issue{}, ErrNotFound
	})
	cases := []struct {
		name, kind, payload string
		related, issue      *string
		want                map[string]bool
	}{
		{"comment related issue", "issue.commented", `{"comment_uid":"` + commentUID + `","reply_to_uid":"00000000000000000000000003","reply_kind":"reply"}`, &related, nil, map[string]bool{"peer": true}},
		{"created", "issue.created", `{"links":[{"to_issue_uid":"` + issueUID + `"}]}`, nil, nil, map[string]bool{"peer": true}},
		{"comment reply", "issue.commented", `{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}`, nil, nil, map[string]bool{"peer": true}},
		{"missing comment reply", "issue.commented", `{"reply_to_uid":"00000000000000000000000003","reply_kind":"reply"}`, nil, nil, map[string]bool{}},
		{"snapshot null links before reply", "issue.snapshot", `{"links":null,"comments":[{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}]}`, nil, nil, map[string]bool{"peer": true}},
		{"close legacy parent before evidence", "issue.closed", `{"parent_short_id":"old1","evidence":[{"type":"duplicate-of","issue_ref":"` + issueUID + `"}]}`, nil, nil, map[string]bool{"peer": true}},
		{"metadata notification", "issue.created", `{"metadata":{"` + notification.MetadataKey("recipient") + `":{"re":"` + commentUID + `"}}}`, nil, nil, map[string]bool{"peer": true}},
		{"diff notification", "issue.metadata_updated", `{"diff":{"` + notification.MetadataKey("recipient") + `":{"from":{},"to":{"re":"` + commentUID + `"}}}}`, nil, nil, map[string]bool{"peer": true}},
		{"link added", "issue.linked", `{"from_uid":"` + ownIssueUID + `","to_uid":"` + otherIssueUID + `"}`, nil, &ownIssue, map[string]bool{"own": true, "other": true}},
		{"link removed", "issue.unlinked", `{"from_uid":"` + ownIssueUID + `","to_uid":"` + otherIssueUID + `"}`, nil, &ownIssue, map[string]bool{"own": true, "other": true}},
		{"subject belongs to peer", "issue.updated", `{}`, nil, &related, map[string]bool{"peer": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projects := map[string]bool{}
			err := RelayEventPeerProjects(RemoteEvent{ProjectUID: "own", Type: tc.kind, Payload: jsontext.Value(tc.payload), RelatedIssueUID: tc.related, IssueUID: tc.issue}, lookup, func(uid string) error { projects[uid] = true; return nil })
			require.NoError(t, err)
			require.Equal(t, tc.want, projects)
		})
	}
}

func TestRelayEventPeerProjectsWithholdsUnresolvedLinks(t *testing.T) {
	lookup := NewEventIssueLookup(func(string) (Issue, error) { return Issue{}, ErrNotFound }, func(string, string) (Issue, error) { return Issue{}, ErrNotFound }, func(string) (Issue, error) { return Issue{}, ErrNotFound })
	for _, tc := range []struct{ kind, payload string }{
		{"issue.linked", `{"from_uid":"missing"}`},
		{"issue.unlinked", `{"to_uid":"missing"}`},
		{"issue.links_changed", `{"related_added_uids":["missing"]}`},
		{"issue.snapshot", `{"links":[{"to_issue_uid":"missing"}]}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			err := RelayEventPeerProjects(RemoteEvent{ProjectUID: "own", Type: tc.kind, Payload: jsontext.Value(tc.payload)}, lookup, func(string) error { return nil })
			require.ErrorIs(t, err, ErrRelayLinkUnresolved)
		})
	}
}

func TestAuthorizeRelayReleaseCrossingEvents(t *testing.T) {
	own, peer := "own-issue", "peer-issue"
	lookup := NewEventIssueLookup(func(uid string) (Issue, error) {
		switch uid {
		case own:
			return Issue{ProjectUID: "own"}, nil
		case peer:
			return Issue{ProjectUID: "peer"}, nil
		default:
			return Issue{}, ErrNotFound
		}
	}, nil, nil)
	denied := errors.New("denied")
	ctx := WithProjectAccessTransactionCheck(t.Context(), func(_ context.Context, _ Transaction, uid string) error {
		if uid == "peer" {
			return denied
		}
		return nil
	})
	for _, kind := range []string{"issue.linked", "issue.unlinked"} {
		event := RemoteEvent{ProjectUID: "own", IssueUID: &own, Type: kind, Payload: jsontext.Value(`{"from_uid":"own-issue","to_uid":"peer-issue"}`)}
		t.Run(kind, func(t *testing.T) {
			require.ErrorIs(t, AuthorizeRelayRelease(ctx, nil, lookup, "own", []RemoteEvent{event}), ErrUnsupportedEventFeatures)
			supported := WithRelayPeerEventFeatures(ctx, RelayCrossLinksEventFeature)
			require.ErrorIs(t, AuthorizeRelayRelease(supported, nil, lookup, "own", []RemoteEvent{event}), denied)
			require.NoError(t, AuthorizeRelayRelease(WithRelayPeerEventFeatures(t.Context(), RelayCrossLinksEventFeature), nil, lookup, "own", []RemoteEvent{event}))
		})
	}
}
