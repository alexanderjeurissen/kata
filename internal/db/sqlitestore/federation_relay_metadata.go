package sqlitestore

import (
	"context"
	"database/sql"
	"errors"

	"go.kenn.io/kata/internal/db"
)

// RelayMetadataRequiresCrossLinks reads checkpoint content without preparing a reset.
func (d *Store) RelayMetadataRequiresCrossLinks(ctx context.Context, projectID int64) (bool, error) {
	tx, err := d.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	project, err := scanProject(tx.QueryRowContext(ctx, projectSelect+` WHERE id=?`, projectID))
	if err != nil {
		return false, err
	}
	pin, err := rootPinTx(ctx, tx, project.UID, "")
	unpinned := errors.Is(err, db.ErrNotFound)
	if err != nil && !unpinned {
		return false, err
	}
	if unpinned {
		binding, err := scanFederationBinding(tx.QueryRowContext(ctx, federationBindingSelect+` WHERE project_id=?`, projectID))
		if err != nil {
			return false, err
		}
		pin.AuthorityUID, err = relayProjectRootTx(ctx, tx, project.UID, binding, d.InstanceUID())
		if err != nil {
			return false, err
		}
		if pin.AuthorityUID == "" {
			return false, db.ErrNotFound
		}
	}
	return db.ScanRelayMetadataFeatures(project.UID, pin.AuthorityUID, unpinned,
		func(after int64) ([]db.Event, error) { return relayCheckpointEventPageTx(ctx, tx, projectID, after) },
		func() ([]db.RelayCheckpointIssue, error) { return d.relayCheckpointIssuesTx(ctx, tx, projectID) },
		func(event db.RemoteEvent) (bool, error) { return relayEventCrossesProjectTx(ctx, tx, projectID, event) },
		func(uid string) (string, error) {
			var projectUID string
			var peerID int64
			err := tx.QueryRowContext(ctx, `SELECT p.uid,p.id FROM issues i JOIN projects p ON p.id=i.project_id WHERE i.uid=?`, uid).Scan(&projectUID, &peerID)
			if errors.Is(err, sql.ErrNoRows) {
				return "", nil
			}
			if err != nil || !unpinned || peerID == projectID {
				return projectUID, err
			}
			shared, err := relaySharedProjectsTx(ctx, tx, projectID, peerID, false)
			if err != nil {
				return "", err
			}
			if !shared {
				return project.UID, nil
			}
			return projectUID, nil
		})
}
