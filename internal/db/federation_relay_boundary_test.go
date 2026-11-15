package db

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/notification"
)

func TestRelayEventPeerProjects(t *testing.T) {
	const issueUID = "00000000000000000000000001"
	const commentUID = "00000000000000000000000002"
	peer := Issue{ProjectUID: "peer"}
	related := issueUID
	lookup := NewEventIssueLookup(func(uid string) (Issue, error) {
		if uid == issueUID {
			return peer, nil
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
		related             *string
		peer                bool
	}{
		{"linked", "issue.linked", `{}`, &related, true},
		{"linked payload", "issue.linked", `{"from_uid":"` + issueUID + `","to_uid":"` + issueUID + `"}`, nil, true},
		{"unlinked payload", "issue.unlinked", `{"from_issue_uid":"` + issueUID + `","to_issue_uid":"` + issueUID + `"}`, nil, true},
		{"created", "issue.created", `{"links":[{"to_issue_uid":"` + issueUID + `"}]}`, nil, true},
		{"moved snapshot notification", "issue.moved", `{"from_project_uid":"source","to_project_uid":"own","snapshot":{"metadata":{"` + notification.MetadataKey("recipient") + `":{"re":"` + commentUID + `"}}}}`, nil, true},
		{"comment reply", "issue.commented", `{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}`, nil, true},
		{"missing comment reply", "issue.commented", `{"reply_to_uid":"00000000000000000000000003","reply_kind":"reply"}`, nil, false},
		{"snapshot null links before reply", "issue.snapshot", `{"links":null,"comments":[{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}]}`, nil, true},
		{"close legacy parent before evidence", "issue.closed", `{"parent_short_id":"old1","evidence":[{"type":"duplicate-of","issue_ref":"` + issueUID + `"}]}`, nil, true},
		{"metadata notification", "issue.created", `{"metadata":{"` + notification.MetadataKey("recipient") + `":{"re":"` + commentUID + `"}}}`, nil, true},
		{"diff notification", "issue.metadata_updated", `{"diff":{"` + notification.MetadataKey("recipient") + `":{"from":{"re":"` + commentUID + `"},"to":{}}}}`, nil, true},
	}
	t.Run("subject moved to peer", func(t *testing.T) {
		projects := map[string]bool{}
		err := RelayEventPeerProjects(RemoteEvent{ProjectUID: "own", Type: "issue.updated", IssueUID: &related, Payload: jsontext.Value(`{}`)}, lookup, func(uid string) error { projects[uid] = true; return nil })
		require.NoError(t, err)
		require.Contains(t, projects, "peer")
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projects := map[string]bool{}
			err := RelayEventPeerProjects(RemoteEvent{ProjectUID: "own", Type: tc.kind, Payload: jsontext.Value(tc.payload), RelatedIssueUID: tc.related}, lookup, func(uid string) error { projects[uid] = true; return nil })
			require.NoError(t, err)
			if tc.peer {
				require.Contains(t, projects, "peer")
			} else {
				require.NotContains(t, projects, "peer")
			}
		})
	}
}

func TestAuthorizeRelayReleaseMovedNotification(t *testing.T) {
	const source = "00000000000000000000000001"
	const destination = "00000000000000000000000002"
	const peer = "00000000000000000000000003"
	const comment = "00000000000000000000000004"
	issue := "00000000000000000000000005"
	event := RemoteEvent{
		EventUID: "00000000000000000000000006", OriginInstanceUID: "00000000000000000000000007",
		ProjectUID: destination, ProjectName: "destination", IssueUID: &issue, Type: "issue.moved", Actor: "member",
		HLCPhysicalMS: 1700000000000, CreatedAt: time.UnixMilli(1700000000000).UTC(),
		Payload: jsontext.Value(`{"issue_uid":"` + issue + `","from_project_uid":"` + source + `","from_short_id":"old1","to_project_uid":"` + destination + `","to_short_id":"new1","snapshot":{"uid":"` + issue + `","title":"Moved issue","author":"member","status":"open","metadata":{"` + notification.MetadataKey("recipient") + `":{"re":"` + comment + `"}}}}`),
	}
	var err error
	event.ContentHash, err = RemoteEventContentHash(event)
	require.NoError(t, err)
	body, err := EncodeRelaySourceEvent(event)
	require.NoError(t, err)
	retained, err := DecodeRelaySourceEvent(body)
	require.NoError(t, err)
	lookup := NewEventIssueLookup(func(string) (Issue, error) { return Issue{ProjectUID: destination}, nil }, nil, func(uid string) (Issue, error) {
		if uid == comment {
			return Issue{ProjectUID: peer}, nil
		}
		return Issue{}, ErrNotFound
	})
	denied := errors.New("peer denied")
	ctx := WithProjectAccessTransactionCheck(t.Context(), func(_ context.Context, _ Transaction, uid string) error {
		if uid == peer {
			return denied
		}
		return nil
	})
	require.ErrorIs(t, AuthorizeRelayRelease(ctx, nil, lookup, destination, []RemoteEvent{retained}), denied)
	require.NoError(t, AuthorizeRelayRelease(t.Context(), nil, lookup, destination, []RemoteEvent{retained}))
}
