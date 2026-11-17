package daemon

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchSharedRequestsLexicalForHybridOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") == "semantic" {
			require.Equal(t, "false", r.URL.Query().Get("include_lexical"))
		} else {
			require.Equal(t, "true", r.URL.Query().Get("include_lexical"))
		}
		_, err := w.Write([]byte(`{"results":[],"lexical":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	client := claimHubClient{baseURL: server.URL, client: server.Client()}
	for _, mode := range []string{"semantic", "hybrid", "auto"} {
		_, _, _, err := client.SearchShared(t.Context(), 1, hybridParams{Query: mode, Requested: mode}, 20)
		require.NoError(t, err)
	}
}

func TestSearchSharedBoundsHubResponse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		length    int64
		wantError bool
	}{
		{name: "both legs fit the bound", body: `{"results":[],"padding":"` + strings.Repeat("x", 2<<20) + `"}`},
		{name: "oversized response", body: `{"results":[],"padding":"` + strings.Repeat("x", 3<<20) + `"}`, wantError: true},
		{name: "declared size", body: "{}", length: 14 << 20, wantError: true},
		{name: "malformed", body: "{", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.body)
			body := &bridgeTrackedResponseBody{Reader: reader}
			transport := &bridgeResponseTransport{body: body, status: http.StatusOK, length: tc.length}
			client := claimHubClient{baseURL: "https://hub.example", client: &http.Client{Transport: transport}}
			hits, _, _, err := client.SearchShared(t.Context(), 1, hybridParams{Query: "test"}, 20)
			if tc.wantError {
				require.Error(t, err)
				if tc.length > 0 {
					require.Equal(t, len(tc.body), reader.Len())
				}
				if tc.name == "oversized response" {
					require.ErrorContains(t, err, "hub search response exceeds")
					require.LessOrEqual(t, len(tc.body)-reader.Len(), 40*(compactRowMaxBytes+hitEnvelopeBytes)+responseEnvelopeBytes+1)
				}
			} else {
				require.NoError(t, err)
				require.Empty(t, hits)
			}
			require.True(t, body.closed)
		})
	}
}
