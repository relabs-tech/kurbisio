package utils

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
