// +build darwin dragonfly freebsd linux openbsd solaris netbsd

package statfs

// get Linux disk usage
func diskUsage(path string) (disk DiskStatus) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(path, &fs)
	if err != nil {
		return
	}

	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}
