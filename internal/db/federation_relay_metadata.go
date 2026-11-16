package db

import (
	"context"
	"encoding/json/jsontext"
)

// RelayMetadataFeatureReader inspects the content a relay may need to receive.
type RelayMetadataFeatureReader interface {
	RelayMetadataRequiresCrossLinks(ctx context.Context, projectID int64) (bool, error)
}

// ScanRelayMetadataFeatures inspects retained events and checkpoint snapshots.
func ScanRelayMetadataFeatures(projectUID, authorityUID string, unpinned bool, page func(int64) ([]Event, error), issues func() ([]RelayCheckpointIssue, error), crosses func(RemoteEvent) (bool, error), resolve func(string) (string, error)) (bool, error) {
	required := func(event RemoteEvent) (bool, error) {
		feature, err := RelayContentRequiredFeatures(event, authorityUID, resolve)
		return feature != "", err
	}
	for after := int64(0); ; {
		events, err := page(after)
		if err != nil {
			return false, err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			remote := RemoteEventFromStored(event)
			crossing, err := crosses(remote)
			if err != nil {
				return false, err
			}
			if !crossing || (unpinned && remote.OriginInstanceUID == authorityUID) {
				needed, err := required(remote)
				if needed || err != nil {
					return needed, err
				}
			}
		}
		after = events[len(events)-1].ID
	}
	snapshots, err := issues()
	if err != nil {
		return false, err
	}
	for _, issue := range snapshots {
		event := RemoteEvent{ProjectUID: projectUID, OriginInstanceUID: authorityUID, IssueUID: &issue.IssueUID, Type: "issue.snapshot", Payload: jsontext.Value(issue.Payload)}
		needed, err := required(event)
		if needed || err != nil {
			return needed, err
		}
	}
	return false, nil
}
