package daemon

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchSharedBoundsHubResponse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		padding   int
		wantError bool
	}{
		{name: "large response", padding: 1 << 20},
		{name: "oversized response", padding: 2 << 20, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := `{"results":[],"padding":"` + strings.Repeat("x", tc.padding) + `"}`
			reader := strings.NewReader(payload)
			body := &bridgeTrackedResponseBody{Reader: reader}
			transport := &bridgeResponseTransport{body: body, status: http.StatusOK}
			client := claimHubClient{baseURL: "https://hub.example", client: &http.Client{Transport: transport}}

			hits, err := client.SearchShared(t.Context(), 1, hybridParams{Query: "test"}, 20)
			if tc.wantError {
				require.ErrorContains(t, err, "hub search response exceeds")
				require.LessOrEqual(t, len(payload)-reader.Len(), 20*(compactRowMaxBytes+hitEnvelopeBytes)+responseEnvelopeBytes+1)
			} else {
				require.NoError(t, err)
				require.Empty(t, hits)
			}
			require.True(t, body.closed)
		})
	}
}

func TestSearchSharedRejectsDeclaredOversizeAndMalformedResponse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		length int64
	}{
		{name: "declared size", body: "{}", length: 14 << 20},
		{name: "malformed", body: "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.body)
			body := &bridgeTrackedResponseBody{Reader: reader}
			client := claimHubClient{baseURL: "https://hub.example", client: &http.Client{Transport: searchResponseTransport{body: body, length: tc.length}}}
			_, err := client.SearchShared(t.Context(), 1, hybridParams{Query: "test"}, 20)
			require.Error(t, err)
			require.True(t, body.closed)
			if tc.length > 0 {
				require.Equal(t, len(tc.body), reader.Len())
			}
		})
	}
}

type searchResponseTransport struct {
	body   *bridgeTrackedResponseBody
	length int64
}

func (t searchResponseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: t.body, ContentLength: t.length, Request: req}, nil
}
