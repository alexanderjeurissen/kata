package dbtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
)

type relayTransientError struct{}

func (relayTransientError) Error() string    { return "retry transaction" }
func (relayTransientError) Code() int        { return 5 }
func (relayTransientError) SQLState() string { return "40001" }

// RunRelayRetryCleanup checks host cleanup before a relay transaction retry.
func RunRelayRetryCleanup(t *testing.T, backend Backend) {
	for _, failCleanup := range []bool{false, true} {
		name := "retry after cleanup"
		if failCleanup {
			name = "stop when cleanup fails"
		}
		t.Run(name, func(t *testing.T) {
			store := backend.Open(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			fixture := NewRelaySharedAccessFixture(t, store)
			binding := fixture.Grant.Enrollment.RelayBindingUID
			timeout, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			pool, ok := store.(interface{ SetMaxOpenConns(int) })
			require.True(t, ok)
			pool.SetMaxOpenConns(1)
			ctx := db.WithRelayPeerEventFeatures(timeout, db.SupportedEventFeatures)
			attempts, cleanups := 0, 0
			cleanupErr := errors.New("cleanup failed")
			fenced := db.WithProjectAccessTransactionCheck(ctx, func(_ context.Context, _ db.Transaction, uid string) error {
				if uid != fixture.Projects[1].UID {
					return nil
				}
				attempts++
				if attempts > 1 {
					require.Equal(t, 1, cleanups)
					return nil
				}
				return fmt.Errorf("peer fence: %w", db.AfterTransactionRollback(relayTransientError{}, func(ctx context.Context) error {
					cleanups++
					_, err := store.ProjectByUID(ctx, fixture.Projects[1].UID)
					if err != nil {
						return err
					}
					if failCleanup {
						return cleanupErr
					}
					return nil
				}))
			})
			before := relayStateForBinding(t, store, binding)
			out, err := store.PendingRelayDeliveries(fenced, binding, db.RelayStreamEvent, 100)
			if failCleanup {
				require.ErrorIs(t, err, db.ErrTransactionFinalizationFailed)
				require.ErrorIs(t, err, cleanupErr)
				require.Empty(t, out)
				require.Equal(t, before, relayStateForBinding(t, store, binding))
				require.Equal(t, 1, attempts)
				require.ErrorIs(t, db.FinishTransactionRollback(ctx, err, nil), cleanupErr)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, out)
				require.Equal(t, 2, attempts)
			}
			require.Equal(t, 1, cleanups)
		})
	}
}
