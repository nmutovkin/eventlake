package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/nmutovkin/eventlake/internal/apikey"
)

type contextKey string

const (
	ctxTenantID contextKey = "tenant_id"
	ctxKeyID    contextKey = "key_id"
)

func TenantIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxTenantID).(string)
	return v
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawKey := extractBearerToken(r)
		if rawKey == "" {
			writeError(w, http.StatusUnauthorized, "missing api key")
			return
		}

		if len(rawKey) < 12 {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}

		prefix := rawKey[:12]
		key, storedHash, err := s.apiKeys.Lookup(r.Context(), prefix)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "auth error")
			return
		}

		if apikey.HashKey(rawKey) != storedHash {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}

		ctx := context.WithValue(r.Context(), ctxTenantID, key.TenantID)
		ctx = context.WithValue(ctx, ctxKeyID, key.ID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractBearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if after, found := strings.CutPrefix(h, "Bearer "); found {
		return after
	}
	return ""
}
