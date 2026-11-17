package daemon

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"go.kenn.io/kata/internal/api"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/pagination"
)

// registerSearchHandlers installs GET /api/v1/projects/{id}/search. Returns the
// spec §4.10 envelope: query echo, effective mode, optional degraded fields,
// and ranked results with mode-scoped score + matched_in. The lexical and
// vector legs run concurrently; see hybridSearch.
func registerSearchHandlers(humaAPI huma.API, cfg ServerConfig) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "searchIssues",
		Description: "Explicit semantic/hybrid modes return a validation error (400) when a configured embedding credential source is unusable or the provider rejects authentication or access. Default search returns lexical results with degraded_reason instead. Transient vector failures return unavailable (503) in explicit modes.",
		Errors:      []int{400, 401, 403, 404, 503},
		Responses: map[string]*huma.Response{
			"default": {Description: "Error"},
		},
		Method: "GET",
		Path:   "/api/v1/projects/{project_id}/search",
	}, func(ctx context.Context, in *api.SearchRequest) (*api.SearchResponse, error) {
		if strings.TrimSpace(in.Query) == "" {
			return nil, api.NewError(400, "validation",
				"query parameter q must be non-empty", "", nil)
		}
		if _, err := activeProjectByID(ctx, cfg.DB, in.ProjectID); err != nil {
			return nil, err
		}
		limit := in.Limit
		if limit <= 0 {
			limit = 20
		}
		mode := in.Mode
		binding, bindingErr := cfg.DB.FederationBindingByProject(ctx, in.ProjectID)
		if bindingErr != nil && !errors.Is(bindingErr, db.ErrNotFound) {
			return nil, internalAPIError(bindingErr)
		}
		direct := bindingErr == nil && binding.IsDirectSpoke()
		if insecureReadonlyRequest(ctx) && (cfg.Embedder != nil || direct) {
			switch mode {
			case "hybrid", "semantic":
				return nil, api.NewError(401, "auth_required",
					"semantic search requires authentication; daemon is in --insecure-readonly mode", "", nil)
			case "", "auto":
				mode = "lexical"
			}
		}

		stable := in.Sort == "oldest"
		if (stable && in.Mode != "lexical") || (in.Cursor != "" && !stable) {
			return nil, api.NewError(400, "validation", "cursor pagination requires mode=lexical and sort=oldest", "", nil)
		}
		probeLimit, err := pageProbeLimit(limit)
		if err != nil {
			return nil, err
		}
		var res hybridResult
		hash := ""
		if stable {
			params := db.SearchFTSParams{ProjectID: in.ProjectID, Query: in.Query, Status: in.Status, IncludeDeleted: in.IncludeDeleted, Labels: in.Labels, ExcludeLabels: in.ExcludeLabels, IssueScope: issueScopeFromContext(ctx), StableOrder: true}
			normalized := params
			normalized.Labels = normalizePageStrings(in.Labels, true)
			normalized.ExcludeLabels = normalizePageStrings(in.ExcludeLabels, true)
			hash = pagination.Fingerprint(struct {
				Instance string
				Filters  db.SearchFTSParams
			}{cfg.DB.InstanceUID(), normalized})
			params.After, err = pagination.Decode(in.Cursor, hash)
			if err != nil {
				return nil, api.NewError(400, "validation", err.Error(), "", nil)
			}
			params.Limit = probeLimit
			res.Hits, err = cfg.DB.SearchFTS(ctx, params)
			res.Mode = modeLexical
		} else {
			params := hybridParams{
				ProjectID: in.ProjectID, Query: in.Query, Limit: limit, ProbeLimit: probeLimit,
				IncludeDeleted: in.IncludeDeleted, Requested: mode,
				Labels: in.Labels, ExcludeLabels: in.ExcludeLabels, Status: in.Status,
				IssueScope: issueScopeFromContext(ctx),
			}
			if direct {
				params.RemoteVector = func(ctx context.Context, p hybridParams, fetch int, lexical []db.SearchCandidate) ([]db.SearchCandidate, []db.SearchCandidate, bool, error) {
					ctx, cancel := context.WithTimeout(ctx, queryEmbedTimeout)
					defer cancel()
					client, cred, err := claimForwardClient(ctx, cfg, binding)
					if err != nil {
						return nil, nil, false, err
					}
					hits, matched, bounded, err := client.SearchShared(ctx, cred.HubProjectID, p, fetch)
					if err != nil {
						return nil, nil, false, err
					}
					candidates, err := hydrateRemoteHits(ctx, cfg.DB, hits, p)
					if err != nil {
						return nil, nil, false, err
					}
					current, err := hydrateRemoteHits(ctx, cfg.DB, matched, p)
					if err != nil {
						return nil, nil, false, err
					}
					byUID := make(map[string]db.SearchCandidate, len(current))
					for _, hit := range current {
						byUID[hit.Issue.UID] = hit
					}
					var unmatched []db.SearchCandidate
					for _, hit := range lexical {
						if _, exists := byUID[hit.Issue.UID]; !exists {
							unmatched = append(unmatched, hit)
						}
					}
					pending, err := pendingSearchIssues(ctx, cfg.DB, binding, unmatched)
					if err != nil {
						return nil, nil, false, err
					}
					for _, hit := range unmatched {
						if pending[hit.Issue.UID] {
							current = append(current, hit)
						}
					}
					return candidates, current, bounded, nil
				}
			}
			res, err = hybridSearch(ctx, cfg.DB, cfg.VectorIndex, cfg.Embedder, params)
		}

		if err != nil {
			if me, ok := errors.AsType[*modeError](err); ok {
				kind := "validation"
				if me.Status() == 503 {
					kind = "unavailable"
				}
				return nil, api.NewError(me.Status(), kind, me.Error(), "", nil)
			}
			return nil, internalAPIError(err)
		}

		more := len(res.Hits) > limit
		page := api.PageMetadata{Complete: !more, Truncated: more}
		if !stable && (res.Mode != modeLexical || res.Degraded || len(res.Hits) >= 200) {
			page.Complete = false
			page.Truncated = true
		}
		if more {
			res.Hits = res.Hits[:limit]
			if stable {
				last := res.Hits[len(res.Hits)-1].Issue
				page.NextCursor = pagination.Encode(pagination.Position{CreatedAt: last.CreatedAt, ID: last.ID}, hash)
			}
		}
		out := &api.SearchResponse{EmbeddingsConfigured: strconv.FormatBool(cfg.Embedder != nil || direct)}
		out.Body.PageMetadata = page
		out.Body.Query = in.Query
		out.Body.Mode = string(res.Mode)
		out.Body.Degraded = res.Degraded
		out.Body.DegradedReason = res.DegradedReason
		out.Body.Results = make([]api.SearchHit, 0, len(res.Hits))
		for _, c := range res.Hits {
			webURL := issueWebURL(cfg, c.Issue.UID)
			if c.Hub {
				webURL = c.WebURL
			}
			out.Body.Results = append(out.Body.Results, api.SearchHit{
				Issue:     c.Issue,
				WebURL:    webURL,
				Remote:    c.Remote,
				Oversized: c.Oversized,
				Score:     c.Score,
				MatchedIn: c.MatchedIn,
			})
		}
		return out, nil
	})
}

func pendingSearchIssues(ctx context.Context, store db.Storage, binding db.FederationBinding, lexical []db.SearchCandidate) (map[string]bool, error) {
	wanted := make(map[string]bool, len(lexical))
	for _, hit := range lexical {
		wanted[hit.Issue.UID] = false
	}
	pending := make(map[string]bool)
	for cursor := binding.PushCursorEventID; len(pending) < len(wanted); {
		events, err := store.PendingFederationPushEvents(ctx, binding.ProjectID, store.InstanceUID(), cursor, 1000)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			if event.IssueUID != nil {
				if _, ok := wanted[*event.IssueUID]; ok && pushEventMaterializesIssue(event, *event.IssueUID) {
					pending[*event.IssueUID] = true
				}
			}
		}
		next := events[len(events)-1].ID
		if next <= cursor {
			break
		}
		cursor = next
	}
	return pending, nil
}
