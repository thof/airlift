package process

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

type Process struct {
	path    string
	pidFile *os.File
	locked  bool
}

func NewProcessInstance(path string) *Process {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
	pidFile, err := openPidfile(path, 0600)
	if err != nil {
		panic(err)
	}
	process := &Process{path: path, pidFile: pidFile}
	process.Refresh()
	return process
}

func (p *Process) Refresh() {
	p.locked = tryLock(p.pidFile)
}

func (p *Process) ClearPid() {
	if !p.locked {
		panic(fmt.Errorf("pid file not locked by us"))
	}
	_, _ = p.pidFile.Seek(0, 0)
	_ = p.pidFile.Truncate(0)
}

func (p *Process) WritePid(pid int) error {
	p.ClearPid()
	if _, err := p.pidFile.WriteString(fmt.Sprintf("%d\n", pid)); err != nil {
		return err
	}
	if err := p.pidFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (p *Process) Alive() bool {
	p.Refresh()
	if p.locked {
		return false
	}
	pid := p.ReadPid()
	if err := syscall.Kill(pid, 0); err != nil {
		panic(fmt.Errorf("signaling pid %d failed: %w", pid, err))
	}
	return true
}

func (p *Process) ReadPid() int {
	if p.locked {
		panic("pid file is locked by us")
	}
	if _, err := p.pidFile.Seek(0, 0); err != nil {
		panic(err)
	}
	reader := bufio.NewReader(p.pidFile)
	line, _, err := reader.ReadLine()
	if err != nil {
		panic(err)
	}
	pid, err := strconv.Atoi(string(line))
	if err != nil {
		panic(fmt.Errorf("pid file '%s' contains garbage: %s", p.path, string(line)))
	}
	if pid <= 0 {
		panic(fmt.Errorf("pid file '%s' contains an invalid pid: %d", p.path, pid))
	}
	return pid
}

func tryLock(f *os.File) bool {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return true
	}
	return false
}

func openPidfile(f string, mode os.FileMode) (*os.File, error) {
	fd, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	return fd, nil
}
