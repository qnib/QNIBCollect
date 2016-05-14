// +build !linux

package collector

// Collect metrics
func (ps ProcStatus) Collect() {
	// This does nothing. Procstatus is a linux-only collector and
	// we don't need to have it on other platforms.
}
