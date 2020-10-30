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

// StringPtr returns a pointer to the string passed as parameter
func StringPtr(str string) *string {
	return &str
}

// IntPtr returns a pointer to the int passed as parameter
func IntPtr(b int) *int {
	return &b
}

// Float64Ptr returns a pointer to the int passed as parameter
func Float64Ptr(f float64) *float64 {
	return &f
}
