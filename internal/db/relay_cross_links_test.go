package db_test

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

func TestRelayCrossLinkFeature(t *testing.T) {
	root, replica := "root", "replica"
	child, foreign := "child", "foreign"
	remote := db.RemoteEvent{OriginInstanceUID: root, ProjectUID: "a", IssueUID: &child, Type: "issue.links_changed", Payload: jsontext.Value(`{"related_added_uids":["foreign"]}`)}
	projects := map[string]string{child: "a", foreign: "b"}
	resolve := func(uid string) (string, error) { return projects[uid], nil }
	feature, err := db.RelayContentRequiredFeatures(remote, root, resolve)
	require.NoError(t, err)
	require.Equal(t, db.RelayCrossLinksEventFeature, feature)
	delete(projects, foreign)
	feature, err = db.RelayContentRequiredFeatures(remote, root, resolve)
	require.NoError(t, err)
	require.Equal(t, db.RelayCrossLinksEventFeature, feature)
	projects[foreign] = "a"
	feature, err = db.RelayContentRequiredFeatures(remote, root, resolve)
	require.NoError(t, err)
	require.Empty(t, feature)
	remote.OriginInstanceUID = replica
	projects[foreign] = "b"
	feature, err = db.RelayContentRequiredFeatures(remote, root, resolve)
	require.NoError(t, err)
	require.Empty(t, feature)
}

func TestRelayCrossLinkParentFold(t *testing.T) {
	root, replica := "root", "replica"
	child, foreign, local := "child", "foreign", "local"
	events := []db.FoldEvent{
		{UID: "1", ProjectUID: "a", IssueUID: child, OriginInstanceUID: root, Type: "issue.links_changed", HLCPhysicalMS: 1, Payload: jsontext.Value(`{"parent_set_uid":"foreign"}`)},
		{UID: "2", ProjectUID: "a", IssueUID: child, OriginInstanceUID: replica, Type: "issue.links_changed", HLCPhysicalMS: 2, Payload: jsontext.Value(`{"parent_set_uid":"local"}`)},
	}
	authority := db.RelayFoldAuthority{
		Projects: map[string]string{child: "a", foreign: "b", local: "a"},
		Roots:    map[string]string{"a": root, "b": root},
		Groups:   map[string]map[string]bool{"a": {"b": true}},
		Relayed:  map[string]bool{"a": true},
	}
	foreignParent := db.FoldLinkKey{FromUID: child, ToUID: foreign, Type: "parent"}
	localParent := db.FoldLinkKey{FromUID: child, ToUID: local, Type: "parent"}
	projection := db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.True(t, projection.Links[foreignParent].Present)
	require.False(t, projection.Links[localParent].Present)
	events[1].Payload = jsontext.Value(`{"parent_set_uid":"local","parent_removed_uid":"foreign","related_added_uids":["local"]}`)
	projection = db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.True(t, projection.Links[foreignParent].Present)
	require.False(t, projection.Links[localParent].Present)
	require.True(t, projection.Links[db.FoldLinkKey{FromUID: child, ToUID: local, Type: "related"}].Present)
	events[1].Payload = jsontext.Value(`{"parent_removed_uid":"foreign"}`)
	projection = db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.True(t, projection.Links[foreignParent].Present)
	events[1].Payload = jsontext.Value(`{"parent_set_uid":"local","parent_removed_uid":"foreign"}`)
	events[1].OriginInstanceUID = root
	projection = db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.False(t, projection.Links[foreignParent].Present)
	require.True(t, projection.Links[localParent].Present)
	events[1].OriginInstanceUID = replica
	delete(authority.Groups["a"], "b")
	projection = db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.False(t, projection.Links[foreignParent].Present)
	require.True(t, projection.Links[localParent].Present)
	authority.Relayed["a"] = false
	projection = db.FoldRelayLinkEvents(events, authority.AllowParentRemoval)
	require.False(t, projection.Links[foreignParent].Present)
	require.True(t, projection.Links[localParent].Present)
}

func TestRelayCrossLinkPolicy(t *testing.T) {
	child, peer := "child", "peer"
	authority := db.RelayFoldAuthority{
		Projects: map[string]string{child: "a", peer: "b"},
		Roots:    map[string]string{"a": "root", "b": "root"},
		Groups:   map[string]map[string]bool{"a": {"b": true}},
	}
	event := db.FoldEvent{ProjectUID: "a", OriginInstanceUID: "root"}
	policy := authority.Policy(event)
	require.True(t, policy.RootOrigin)
	require.True(t, policy.CrossProject(peer))
	require.False(t, policy.CrossProject(child))
	require.False(t, policy.CrossProject("unknown"))
	require.True(t, policy.SameRoot(peer))

	authority.Roots["b"] = "other"
	require.False(t, authority.Policy(event).SameRoot(peer))
	authority.Roots["b"] = "root"
	delete(authority.Groups["a"], "b")
	require.False(t, authority.Policy(event).SameRoot(peer))
	event.OriginInstanceUID = "replica"
	require.False(t, authority.Policy(event).RootOrigin)
}
