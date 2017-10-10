package statfs

type DiskStatus struct {
	All  uint64 `json:"all"`
	Used uint64 `json:"used"`
	Free uint64 `json:"free"`
}

// get disk usage
func DiskStatfs(path string) DiskStatus {
	return diskUsage(path)
}
