/*
Package util is catchall for all utilities that might be used throughout the fullerite code.

iptools.go:
It includes functionality to determine the ip address of the machine that's running a fullerite instance.

mesos_leader.go:
Detects the leader from amongst a set of mesos masters. It also caches this value for a configurable ttl to save time.

*/
package util
