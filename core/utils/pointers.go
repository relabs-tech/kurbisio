package utils

import "time"

// BeginningOfDay return a timestamp at the beginnig of the day in the timestamp's timezone
func BeginningOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

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
