/*
Copyright 2026 The llm-d-stream-handler-plugin Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package streamhandler

import "context"

// Provisioner gets a running handler for a session and gives it back when the
// session ends.
//
// The interface exists to keep one decision open. JobProvisioner creates a Job
// per session and deletes it on release, which pays a pod cold start -- image
// pull plus Python and OpenCV import -- on every Start. A pooled implementation
// would instead lease from a pre-warmed set and return the handler on release,
// trading that latency for a cross-replica lease protocol (the EPP runs multiple
// replicas, so two of them can otherwise hand out the same warm handler) and for
// a handler that can receive an assignment after it has started, which today's
// handler cannot: it has no listener and reads its whole configuration from env.
//
// Acquire must be idempotent. A retried or duplicated trigger for a session that
// already has a handler is a success, not a conflict. Release must likewise
// tolerate a session that has no handler, so a duplicate stop is harmless.
type Provisioner interface {
	// Acquire makes a handler running for a.SessionID, or returns an error if it
	// cannot. It is safe to call more than once for the same session.
	Acquire(ctx context.Context, a Assignment) error
	// Release ends the handler for sessionID. A session with no handler is not an
	// error.
	Release(ctx context.Context, sessionID string) error
}
