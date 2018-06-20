/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package taskhawk

// Error strings used within Taskhawk
// Error strings are used instead of errors, cause the error stack trace can only be reliably constructed
// from within the appropriate execution context
const (
	ErrStringTaskNotFound = "Task not found"
)
