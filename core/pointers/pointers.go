// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package pointers

import "time"

// SafeInt64 returns the value from ptr or 0 if the pointer is nil
func SafeInt64(ptr *int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

// SafeBool returns the value from ptr or false if the pointer is nil
func SafeBool(ptr *bool) bool {
	if ptr != nil {
		return *ptr
	}
	return false
}

// SafeFloat64 returns the value from ptr or 0 if the pointer is nil
func SafeFloat64(ptr *float64) float64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

// SafeString returns the value from ptr or "" if the pointer is nil
func SafeString(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

// StringPtr returns a pointer to the string passed as parameter
func StringPtr(str string) *string {
	return &str
}

// StringPtrOrEmpty returns the passed pointer or a pointer to the empty string if str is nil
func StringPtrOrEmpty(str *string) *string {
	if str == nil {
		return StringPtr("")
	}
	return str
}

// TimePtrOrEmpty returns the passed pointer or a pointer to time.Time{} if t is nil
func TimePtrOrEmpty(t *time.Time) *time.Time {
	if t == nil {
		return &time.Time{}
	}
	return t
}

// SafeTime returns the value from t or time.Time{} if the pointer is nil
func SafeTime(t *time.Time) time.Time {
	if t != nil {
		return *t
	}
	return time.Time{}
}

// TimePtr returns a pointer to a t
func TimePtr(t time.Time) *time.Time {
	return &t
}

// IntPtr returns a pointer to the int passed as parameter
func IntPtr(d int) *int {
	return &d
}

// Int64Ptr returns a pointer to the int passed as parameter
func Int64Ptr(d int64) *int64 {
	return &d
}

// Float64Ptr returns a pointer to the int passed as parameter
func Float64Ptr(f float64) *float64 {
	return &f
}

// BoolPtr returns a pointer to the bool passed as parameter
func BoolPtr(b bool) *bool {
	return &b
}

var (
	// True is true
	True bool = true
	// False is true
	False bool = false
)
