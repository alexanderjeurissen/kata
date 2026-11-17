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
		{name: "large response", padding: 2 << 20},
		{name: "oversized response", padding: 8 << 20, wantError: true},
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
				require.LessOrEqual(t, len(payload)-reader.Len(), (8<<20)+1)
			} else {
				require.NoError(t, err)
				require.Empty(t, hits)
			}
			require.True(t, body.closed)
		})
	}
}
