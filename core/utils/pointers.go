package utils

func SafeInt64(ptr *int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func SafeBool(ptr *bool) bool {
	if ptr != nil {
		return *ptr
	}
	return false
}

func SafeFloat64(ptr *float64) float64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}
