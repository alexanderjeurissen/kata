package db

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/notification"
)

func TestRelayEventPeerProjects(t *testing.T) {
	const issueUID = "00000000000000000000000001"
	const commentUID = "00000000000000000000000002"
	const otherIssueUID = "00000000000000000000000004"
	peer := Issue{ProjectUID: "peer"}
	related := issueUID
	lookup := NewEventIssueLookup(func(uid string) (Issue, error) {
		if uid == issueUID {
			return peer, nil
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
		peer                bool
	}{
		{"comment related issue", "issue.commented", `{"comment_uid":"` + commentUID + `","reply_to_uid":"00000000000000000000000003","reply_kind":"reply"}`, &related, nil, true},
		{"created", "issue.created", `{"links":[{"to_issue_uid":"` + issueUID + `"}]}`, nil, nil, true},
		{"comment reply", "issue.commented", `{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}`, nil, nil, true},
		{"missing comment reply", "issue.commented", `{"reply_to_uid":"00000000000000000000000003","reply_kind":"reply"}`, nil, nil, false},
		{"snapshot null links before reply", "issue.snapshot", `{"links":null,"comments":[{"reply_to_uid":"` + commentUID + `","reply_kind":"reply"}]}`, nil, nil, true},
		{"close legacy parent before evidence", "issue.closed", `{"parent_short_id":"old1","evidence":[{"type":"duplicate-of","issue_ref":"` + issueUID + `"}]}`, nil, nil, true},
		{"metadata notification", "issue.created", `{"metadata":{"` + notification.MetadataKey("recipient") + `":{"re":"` + commentUID + `"}}}`, nil, nil, true},
		{"diff notification", "issue.metadata_updated", `{"diff":{"` + notification.MetadataKey("recipient") + `":{"from":{},"to":{"re":"` + commentUID + `"}}}}`, nil, nil, true},
		{"link added", "issue.linked", `{"from_uid":"` + issueUID + `","to_uid":"` + otherIssueUID + `"}`, nil, nil, true},
		{"link removed", "issue.unlinked", `{"from_uid":"` + issueUID + `","to_uid":"` + otherIssueUID + `"}`, nil, nil, true},
		{"subject moved to peer", "issue.updated", `{}`, nil, &related, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projects := map[string]bool{}
			err := RelayEventPeerProjects(RemoteEvent{ProjectUID: "own", Type: tc.kind, Payload: jsontext.Value(tc.payload), RelatedIssueUID: tc.related, IssueUID: tc.issue}, lookup, func(uid string) error { projects[uid] = true; return nil })
			require.NoError(t, err)
			if tc.peer {
				require.Contains(t, projects, "peer")
				if tc.name == "link added" || tc.name == "link removed" {
					require.Contains(t, projects, "other")
				}
			} else {
				require.Empty(t, projects)
			}
		})
	}
}
