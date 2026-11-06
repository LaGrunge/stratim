// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows

package local

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

func newCmd(ctx context.Context, binary string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, binary, args...)

	// this make sure kill signal to command does not propagate to agent while having no orphans by ...
	// ... create new process group for the command.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// the command can have never been started (e.g. the workflow got canceled
		// before), so there is no process group to kill.
		if cmd.Process == nil {
			return nil
		}
		// ... send kill to whole process group.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	return cmd
}

// processGroupGoneTimeout bounds how long a step's end waits for its killed
// process group to be gone.
const processGroupGoneTimeout = 5 * time.Second

// processGroupPoll is how often that is checked.
const processGroupPoll = 20 * time.Millisecond

// waitProcessGroupGone waits until no process of the group cmd leads runs
// any more, the kill delivered, ctx done or the timeout passed: what runs
// after the step, a finalizer collecting its logs, must not find it still
// running. A killed process only left to be reaped does not count: without
// an init reaping orphans it would stay a zombie.
func waitProcessGroupGone(ctx context.Context, cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	group := cmd.Process.Pid
	timeout := time.NewTimer(processGroupGoneTimeout)
	defer timeout.Stop()
	poll := time.NewTicker(processGroupPoll)
	defer poll.Stop()
	for processGroupAlive(group) {
		select {
		case <-ctx.Done():
			return
		case <-timeout.C:
			log.Warn().Int("group", group).Msgf("processes of a killed step still run after %s", processGroupGoneTimeout)
			return
		case <-poll.C:
		}
	}
}

// processGroupAlive reports whether a process of the process group still
// runs, zombies aside. Where /proc lists the processes (Linux) it tells;
// elsewhere a signal 0 to the group does (see signalGroupAlive).
func processGroupAlive(group int) bool {
	if alive, ok := procGroupAlive("/proc", group); ok {
		return alive
	}
	return signalGroupAlive(syscall.Kill, group)
}

// signalGroupAlive reports whether kill finds a process of the group to
// signal: zombies counted, and a group no longer ours to signal (EPERM) is
// not the step's any more.
func signalGroupAlive(kill func(int, syscall.Signal) error, group int) bool {
	return kill(-group, 0) == nil
}

// procGroupAlive reads procRoot (/proc) for a process of the process group
// that is not a zombie; ok is false without one to read.
func procGroupAlive(procRoot string, group int) (alive, ok bool) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return false, false
	}
	groupID := strconv.Itoa(group)
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join(procRoot, entry.Name(), "stat"))
		if err != nil {
			continue // it ended meanwhile
		}
		// pid (comm) state ppid group ...: comm may hold spaces and parens
		end := strings.LastIndexByte(string(stat), ')')
		if end < 0 {
			continue
		}
		fields := strings.Fields(string(stat[end+1:]))
		if len(fields) < 3 || fields[2] != groupID {
			continue
		}
		if state := fields[0]; state != "Z" && state != "X" {
			return true, true
		}
	}
	return false, true
}
