// Copyright 2026 LaGrunge
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

package explain

import (
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// The rules match on plain substrings and prefixes; the few regular
// expressions are Go's, which run in time linear in the line.

// rule finds one kind of cause; match gives nil when it finds none.
type rule struct {
	kind  string
	match func(*Input) *Cause
}

// rules are tried in this order; the first that matches gives the first
// cause. What the state of the step says comes first, then the failures of
// tests, whose output may hold compiler errors and crashes of their own, then
// the failures of builds and of packaging.
var rules = []rule{
	{"oom", oom},
	{"timeout", timeout},
	{"error", stepError},
	{"mtr", mtr},
	{"panic", goPanic},
	{"gtest", gtest},
	{"crash", crash},
	{"ctest", ctest},
	{"cmake", cmake},
	{"ninja", ninja},
	{"compile", compile},
	{"dh_install", dhInstall},
	{"debhelper", debhelper},
}

const (
	// An excerpt of the end of a log has its last tailLines lines with
	// text: what the step did when it ended.
	tailLines = 5
	// An excerpt has linesBefore lines before the line it is at and
	// linesAfter after it.
	linesBefore = 2
	linesAfter  = 3
	// An excerpt has outputLines lines of output after the line that
	// starts it: of a crash, of a ctest test, of a command ninja ran.
	outputLines = 7
	// The goroutine trace of a Go panic starts within goroutineLines lines
	// after it.
	goroutineLines = 30
	// At most ninjaOutputLines lines of the output of a command ninja ran
	// are looked at for its error.
	ninjaOutputLines = 300
)

// GenericKind is the kind of the cause given when no rule matches.
const GenericKind = "exit"

// Kinds are the kinds of causes, in the order of the rules, the generic
// one last.
func Kinds() []string {
	kinds := make([]string, 0, len(rules)+1)
	for _, r := range rules {
		kinds = append(kinds, r.kind)
	}
	return append(kinds, GenericKind)
}

// find is the index of the first line from i on that matches, -1 if none
// does.
func (in *Input) find(from int, match func(string) bool) int {
	for i := max(from, 0); i < len(in.Lines); i++ {
		if match(in.Lines[i].Text) {
			return i
		}
	}
	return -1
}

// span are the lines from before lines before i to after lines after it.
func (in *Input) span(i, before, after int) []Line {
	return in.Lines[max(i-before, 0):min(i+after+1, len(in.Lines))]
}

// lastLines are the last tailLines lines with text.
func (in *Input) lastLines() []Line {
	var out []Line
	for i := len(in.Lines) - 1; i >= 0 && len(out) < tailLines; i-- {
		if strings.TrimSpace(in.Lines[i].Text) != "" {
			out = append(out, in.Lines[i])
		}
	}
	slices.Reverse(out)
	return out
}

// lineOf is the number of the last of the lines, 0 if there are none.
func lineOf(lines []Line) int {
	if len(lines) == 0 {
		return 0
	}
	return lines[len(lines)-1].N
}

func containsAny(marks ...string) func(string) bool {
	return func(s string) bool {
		for _, m := range marks {
			if strings.Contains(s, m) {
				return true
			}
		}
		return false
	}
}

func hasPrefix(prefixes ...string) func(string) bool {
	return func(s string) bool {
		s = strings.TrimLeft(s, " ")
		for _, p := range prefixes {
			if strings.HasPrefix(s, p) {
				return true
			}
		}
		return false
	}
}

// more is " (+n more)" for n > 0.
func more(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf(" (+%d more)", n)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// oomMarks are what compilers, linkers, runtimes and the kernel print when a
// process runs out of memory or is killed for it. A word like bad_alloc or
// ENOMEM alone is no mark: compilers quote code and tests expect them.
var oomMarks = []string{
	"Killed signal terminated program",
	"internal compiler error: Killed",
	"virtual memory exhausted",
	"terminated with signal 9",
	"terminate called after throwing an instance of 'std::bad_alloc'",
	"fork: Cannot allocate memory",
	"cc1plus: out of memory",
	"cc1: out of memory",
	"java.lang.OutOfMemoryError",
	"JavaScript heap out of memory",
	"fatal error: runtime: out of memory",
	"Out of memory: Killed process",
}

// isOOM tells a line that says a process ran out of memory: a mark, or the
// shell telling of a process it lost to SIGKILL ("line 3:  4242 Killed").
func isOOM(s string) bool {
	if quoted(s) {
		return false
	}
	if strings.Contains(s, ": line ") && strings.Contains(s, " Killed ") {
		// a process the script killed itself is not out of memory
		return !containsAny(selfKills...)(s)
	}
	return containsAny(oomMarks...)(s)
}

// selfKills are commands that send SIGKILL themselves.
var selfKills = []string{"-s KILL", "-s SIGKILL", "-s 9 ", "-KILL", "-9 ", "--signal=KILL", "--signal=9"}

// quoted tells a line that quotes rather than reports: the source line a
// compiler echoes under an error ("  188 |   code"), and cmake's status.
func quoted(s string) bool {
	t := strings.TrimLeft(s, " \t")
	if strings.HasPrefix(t, "| ") || strings.HasPrefix(s, "-- ") {
		return true
	}
	digits := 0
	for digits < len(t) && t[digits] >= '0' && t[digits] <= '9' {
		digits++
	}
	return digits > 0 && strings.HasPrefix(t[digits:], " | ")
}

// oomExitCode is the exit code of a process killed by SIGKILL, as the OOM
// killer kills.
const oomExitCode = 137

func oom(in *Input) *Cause {
	if i := in.find(0, isOOM); i >= 0 {
		return &Cause{
			Title:      "out of memory: " + strings.TrimSpace(in.Lines[i].Text),
			Line:       in.Lines[i].N,
			Confidence: High,
			Excerpt:    in.span(i, linesBefore, linesAfter),
		}
	}
	if strings.Contains(in.Error, "oom kill") {
		tail := in.lastLines()
		return &Cause{
			Title:      "the step was killed for running out of memory (OOM)",
			Line:       lineOf(tail),
			Confidence: High,
			Excerpt:    tail,
		}
	}
	if in.ExitCode == oomExitCode && in.State == model.StatusFailure {
		tail := in.lastLines()
		return &Cause{
			Title:      "the step was killed by SIGKILL (exit code 137), most likely for running out of memory",
			Line:       lineOf(tail),
			Confidence: Medium,
			Excerpt:    tail,
		}
	}
	return nil
}

func timeout(in *Input) *Cause {
	if in.State != model.StatusTimeout && !strings.Contains(in.Error, "timed out after") {
		return nil
	}
	title := "the step ran into its timeout"
	if _, after, ok := strings.Cut(in.Error, "timed out after "); ok {
		title += " of " + after
	}
	tail := in.lastLines()
	return &Cause{Title: title, Line: lineOf(tail), Confidence: High, Excerpt: tail}
}

func stepError(in *Input) *Cause {
	if in.State != model.StatusError || strings.TrimSpace(in.Error) == "" {
		return nil
	}
	msg, _, _ := strings.Cut(strings.TrimSpace(in.Error), "\n")
	if strings.HasPrefix(msg, "uuid=") {
		if _, after, ok := strings.Cut(msg, ": "); ok {
			msg = after
		}
	}
	tail := in.lastLines()
	return &Cause{Title: "the step could not run: " + msg, Line: lineOf(tail), Confidence: High, Excerpt: tail}
}

// mtrResult tells a line with the result of an MTR test: "main.t  w1 [ pass ]".
var mtrResult = containsAny("[ pass ]", "[ fail ]", "[ skipped ]", "[ disabled ]", "[ retry-pass ]", "[ retry-fail ]")

func mtr(in *Input) *Cause {
	var failing []string
	if i := lastIndex(in, hasPrefix("Failing test(s):")); i >= 0 {
		_, list, _ := strings.Cut(in.Lines[i].Text, ":")
		failing = strings.Fields(list)
	}
	// without the list, a run that completed with all tests successful had
	// none fail, whatever failed and passed on a retry
	// without the list, the run of the last failure that completed with all
	// tests successful had none fail, whatever failed and passed on a retry;
	// a later run that did says nothing of it
	lastFail := lastIndex(in, func(s string) bool { return strings.Contains(s, "[ fail ]") })
	allPassed := false
	if len(failing) == 0 && lastFail >= 0 {
		for _, l := range in.Lines[lastFail+1:] {
			if mtrRunStart(l.Text) {
				break
			}
			if strings.HasPrefix(l.Text, "Completed: All ") && strings.Contains(l.Text, "successful") {
				allPassed = true
				break
			}
		}
	}
	first := -1
	for i, l := range in.Lines {
		if allPassed {
			break
		}
		if !strings.Contains(l.Text, "[ fail ]") {
			continue
		}
		name := strings.Fields(l.Text)[0]
		if len(failing) > 0 && !slices.Contains(failing, name) {
			continue
		}
		if passedOnRetry(in, i) {
			continue
		}
		first = i
		break
	}
	if first < 0 {
		i := in.find(0, containsAny("mysql-test-run: *** ERROR:"))
		if i < 0 {
			return nil
		}
		_, msg, _ := strings.Cut(in.Lines[i].Text, "*** ERROR:")
		return &Cause{
			Title:      "MTR: " + strings.TrimSpace(msg),
			Line:       in.Lines[i].N,
			Confidence: Medium,
			Excerpt:    in.span(i, linesBefore, linesAfter),
		}
	}

	text := in.Lines[first].Text
	name := strings.Fields(text)[0]
	title := "MTR: " + name + " failed"
	if _, reason, _ := strings.Cut(text, "[ fail ]"); strings.TrimSpace(reason) != "" {
		title += ": " + strings.TrimSpace(reason)
	} else if j := in.find(first+1, containsAny("mysqltest: ")); j >= 0 && j < first+40 && !anyResultBetween(in, first, j) {
		_, msg, _ := strings.Cut(in.Lines[j].Text, "mysqltest: ")
		title += ": " + strings.TrimSpace(msg)
	}
	title += more(len(failing) - 1)

	excerpt := []Line{in.Lines[first]}
	for i := first + 1; i < len(in.Lines) && len(excerpt) < maxExcerptLines; i++ {
		t := in.Lines[i].Text
		if mtrResult(t) || strings.HasPrefix(t, "Failing test(s):") || strings.HasPrefix(t, "Completed:") {
			break
		}
		if strings.TrimSpace(t) != "" {
			excerpt = append(excerpt, in.Lines[i])
		}
	}
	return &Cause{Title: title, Line: in.Lines[first].N, Confidence: High, Excerpt: excerpt}
}

// passedOnRetry tells whether the MTR test that failed at line i passed when
// it was run again.
func passedOnRetry(in *Input, i int) bool {
	test := mtrTest(in.Lines[i].Text)
	for _, l := range in.Lines[i+1:] {
		if strings.Contains(l.Text, "[ retry-pass ]") && mtrTest(l.Text) == test {
			return true
		}
	}
	return false
}

// mtrRunStart tells a line that starts a run of MTR.
func mtrRunStart(s string) bool {
	return strings.HasPrefix(s, "Logging: ") ||
		containsAny("mysql-test-run.pl", "mariadb-test-run.pl", "./mtr ", "/mtr ")(s)
}

// mtrTest is the test of a line with a result of MTR with its combination:
// "main.t 'row'" of "main.t 'row'   w1 [ fail ]".
func mtrTest(s string) string {
	var test []string
	for f := range strings.FieldsSeq(s) {
		if strings.HasPrefix(f, "[") {
			break
		}
		if len(f) > 1 && f[0] == 'w' && strings.Trim(f[1:], "0123456789") == "" { // the worker
			continue
		}
		test = append(test, f)
	}
	return strings.Join(test, " ")
}

func anyResultBetween(in *Input, from, to int) bool {
	for i := from + 1; i < to; i++ {
		if mtrResult(in.Lines[i].Text) {
			return true
		}
	}
	return false
}

func lastIndex(in *Input, match func(string) bool) int {
	for i, l := range slices.Backward(in.Lines) {
		if match(l.Text) {
			return i
		}
	}
	return -1
}

// goroutineWithin tells whether a goroutine trace starts within n lines
// after i, as it does after a Go panic.
func goroutineWithin(in *Input, i, n int) bool {
	for j := i + 1; j < min(i+n+1, len(in.Lines)); j++ {
		if strings.HasPrefix(in.Lines[j].Text, "goroutine ") {
			return true
		}
	}
	return false
}

func goPanic(in *Input) *Cause {
	for i, l := range in.Lines {
		var what string
		switch {
		case strings.HasPrefix(l.Text, "panic: "):
			what = "Go panic: " + strings.TrimPrefix(l.Text, "panic: ")
		case strings.HasPrefix(l.Text, "fatal error: "):
			what = "Go fatal error: " + strings.TrimPrefix(l.Text, "fatal error: ")
		default:
			continue
		}
		if !goroutineWithin(in, i, goroutineLines) {
			continue
		}
		var excerpt []Line
		for j := i; j < len(in.Lines) && len(excerpt) < outputLines+1; j++ {
			if strings.TrimSpace(in.Lines[j].Text) != "" {
				excerpt = append(excerpt, in.Lines[j])
			}
		}
		return &Cause{Title: what, Line: l.N, Confidence: High, Excerpt: excerpt}
	}
	return nil
}

const gtestFailed = "[  FAILED  ] "

// The assertion a gtest test failed at is looked for at most
// gtestDetailLines lines before the test's end.
const gtestDetailLines = 200

func gtest(in *Input) *Cause {
	// a ctest run that ended with all tests passed, e.g. repeated until they
	// did, makes the failures before it none
	from := lastIndex(in, func(s string) bool { return strings.Contains(s, "% tests passed, 0 tests failed out of ") }) + 1
	var names []string
	first := -1
	for i := from; i < len(in.Lines); i++ {
		l := in.Lines[i]
		rest, ok := strings.CutPrefix(l.Text, gtestFailed)
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		name := strings.TrimSuffix(fields[0], ",")
		if name == "" || (name[0] >= '0' && name[0] <= '9') { // "2 tests, listed below:"
			continue
		}
		if !slices.Contains(names, name) {
			names = append(names, name)
			if first < 0 {
				first = i
			}
		}
	}
	if first < 0 {
		return nil
	}
	name := names[0]

	// the assertion that failed: after the test started, before it failed
	detail := -1
	for i := first - 1; i >= max(first-gtestDetailLines, 0); i-- {
		t := in.Lines[i].Text
		if strings.HasPrefix(t, "[ RUN      ] ") || strings.HasPrefix(t, "[==========]") ||
			strings.HasPrefix(t, "[       OK ]") || strings.HasPrefix(t, gtestFailed) {
			break
		}
		if strings.HasSuffix(t, ": Failure") || strings.Contains(t, ": error: ") {
			detail = i
		}
	}
	title := "gtest: " + name + " failed"
	cause := &Cause{Line: in.Lines[first].N, Confidence: High}
	if detail >= 0 {
		where := strings.TrimSuffix(in.Lines[detail].Text, ": Failure")
		if where != in.Lines[detail].Text && where != "unknown file" {
			title += " at " + path.Base(where)
		}
		cause.Line = in.Lines[detail].N
		cause.Excerpt = slices.Clone(in.Lines[detail:min(first, detail+maxExcerptLines-1)])
	}
	cause.Excerpt = append(cause.Excerpt, in.Lines[first])
	cause.Title = title + more(len(names)-1)
	return cause
}

// crashMarks are what C and C++ runtimes, sanitizers and shells print on a
// crash.
var crashMarks = []string{
	"AddressSanitizer",
	"LeakSanitizer",
	"ThreadSanitizer",
	"MemorySanitizer",
	"UndefinedBehaviorSanitizer",
	": runtime error: ",
	"Segmentation fault",
	"terminate called after throwing an instance of",
	"terminate called without an active exception",
	"*** stack smashing detected ***",
	"double free or corruption",
	"free(): invalid pointer",
	"' failed.", // glibc: "prog: file.c:12: main: Assertion `x' failed."
	"got signal 11",
	"got signal 6",
	"Aborted (core dumped)",
}

func crash(in *Input) *Cause {
	i := in.find(0, func(s string) bool {
		if quoted(s) || strings.HasPrefix(strings.TrimSpace(s), "panic: ") {
			return false // a quote, or the panic rule's
		}
		if strings.Contains(s, "' failed.") {
			return strings.Contains(s, "Assertion `")
		}
		return containsAny(crashMarks...)(s)
	})
	if i < 0 {
		return nil
	}
	return &Cause{
		Title:      "crash: " + strings.TrimSpace(in.Lines[i].Text),
		Line:       in.Lines[i].N,
		Confidence: High,
		Excerpt:    in.span(i, 1, outputLines),
	}
}

// ctestFailedTest is a line of the list of ctest's failed tests:
// "	  3 - unit_foo (Failed)".
var ctestFailedTest = regexp.MustCompile(`^\s*(\d+) - (\S+) \(([^()]+)\)`)

func ctest(in *Input) *Cause {
	header := in.find(0, containsAny("The following tests FAILED:"))
	if header < 0 {
		i := in.find(0, containsAny("Errors while running CTest", "No tests were found!!!"))
		if i < 0 {
			return nil
		}
		return &Cause{
			Title:      "ctest: " + strings.TrimSpace(in.Lines[i].Text),
			Line:       in.Lines[i].N,
			Confidence: Medium,
			Excerpt:    in.span(i, linesBefore, linesAfter),
		}
	}
	type failedTest struct{ number, name, status string }
	var failed []failedTest
	list := []Line{in.Lines[header]}
	for i := header + 1; i < len(in.Lines); i++ {
		var m []string
		if strings.Contains(in.Lines[i].Text, " - ") {
			m = ctestFailedTest.FindStringSubmatch(in.Lines[i].Text)
		}
		if m == nil {
			break
		}
		failed = append(failed, failedTest{m[1], m[2], m[3]})
		list = append(list, in.Lines[i])
	}
	if len(failed) == 0 {
		return &Cause{Title: "ctest: tests failed", Line: in.Lines[header].N, Confidence: Medium, Excerpt: list}
	}
	f := failed[0]
	title := fmt.Sprintf("ctest: %s failed (%s)%s", f.name, f.status, more(len(failed)-1))
	run := in.find(0, func(s string) bool {
		return strings.Contains(s, "#"+f.number+": "+f.name+" ") && strings.Contains(s, "***")
	})
	if run < 0 || run > header {
		return &Cause{Title: title, Line: list[1].N, Confidence: High, Excerpt: list}
	}
	return &Cause{Title: title, Line: in.Lines[run].N, Confidence: High, Excerpt: in.span(run, 0, outputLines)}
}

func cmake(in *Input) *Cause {
	i := in.find(0, hasPrefix("CMake Error"))
	if i < 0 {
		return nil
	}
	head := strings.TrimSpace(in.Lines[i].Text)
	excerpt := []Line{in.Lines[i]}
	var msg string
	for j := i + 1; j < len(in.Lines) && len(excerpt) < 8; j++ {
		t := in.Lines[j].Text
		if strings.HasPrefix(t, "-- ") || strings.HasPrefix(t, "CMake ") {
			break
		}
		if strings.TrimSpace(t) == "" {
			if msg != "" {
				break
			}
			continue
		}
		if msg == "" {
			msg = strings.TrimSpace(t)
		}
		excerpt = append(excerpt, in.Lines[j])
	}
	title := "cmake: "
	if where, ok := strings.CutPrefix(head, "CMake Error at "); ok {
		where = strings.TrimSuffix(where, ":")
		if msg == "" {
			msg = "error"
		}
		title += msg + " (at " + where + ")"
	} else {
		_, rest, _ := strings.Cut(head, ": ")
		if rest == "" {
			rest = msg
		}
		title += rest
	}
	return &Cause{Title: title, Line: in.Lines[i].N, Confidence: High, Excerpt: excerpt}
}

// compilerError is an error of gcc or clang: "src/a.cc:12:5: error: ...".
var compilerError = regexp.MustCompile(`(?:^|\s)(\S+?):(\d+):(?:\d+:)? (?:fatal )?error: (.*)`)

// linkerError is an error of a linker at a line of a source:
// "src/a.cc:12: undefined reference to `f()'".
var linkerError = regexp.MustCompile(`(?:^|\s)(\S+?):(\d+): ((?:undefined reference to|multiple definition of) .*)`)

// matchCompilerError matches compilerError on a line that has its marker.
func matchCompilerError(s string) []string {
	if !strings.Contains(s, "error: ") {
		return nil
	}
	return compilerError.FindStringSubmatch(s)
}

// matchLinkerError matches linkerError on a line that has its marker.
func matchLinkerError(s string) []string {
	if !strings.Contains(s, "undefined reference to") && !strings.Contains(s, "multiple definition of") {
		return nil
	}
	return linkerError.FindStringSubmatch(s)
}

// linkerMarks are what linkers print on an error.
var linkerMarks = []string{"undefined reference to", "ld: error: ", "ld.lld: error: ", "multiple definition of", "ld returned 1 exit status"}

// buildError tells a line with an error of a compiler or a linker, and how to
// put it in a title.
func buildError(s string) (string, bool) {
	if m := matchCompilerError(s); m != nil {
		return path.Base(m[1]) + ":" + m[2] + ": " + m[3], true
	}
	if m := matchLinkerError(s); m != nil {
		return path.Base(m[1]) + ":" + m[2] + ": " + m[3], true
	}
	if containsAny(linkerMarks...)(s) || strings.Contains(s, ": error: ") || strings.Contains(s, ": fatal error: ") {
		return strings.TrimSpace(s), true
	}
	return "", false
}

// ninjaProgress is a line of ninja's progress: "[12/345] Building ...".
var ninjaProgress = regexp.MustCompile(`^\[\d+/\d+\] `)

func ninja(in *Input) *Cause {
	i := in.find(0, hasPrefix("FAILED: "))
	if i < 0 {
		j := in.find(0, hasPrefix("ninja: error: "))
		if j < 0 {
			return nil
		}
		return &Cause{
			Title:      strings.TrimSpace(in.Lines[j].Text),
			Line:       in.Lines[j].N,
			Confidence: High,
			Excerpt:    in.span(j, linesBefore, linesAfter),
		}
	}
	target := strings.TrimSpace(strings.TrimLeft(in.Lines[i].Text, " ")[len("FAILED: "):])
	if strings.HasPrefix(target, "[code=") { // ninja 1.12 gives the exit code
		if _, rest, ok := strings.Cut(target, "] "); ok {
			target = rest
		}
	}
	if fields := strings.Fields(target); len(fields) > 0 {
		target = path.Base(fields[0])
	}

	title := "ninja: " + target + " failed"
	// the output of the command that failed ends where ninja goes on
	end := min(len(in.Lines), i+ninjaOutputLines)
	for j := i + 1; j < end; j++ {
		t := in.Lines[j].Text
		if strings.HasPrefix(t, "FAILED: ") || strings.HasPrefix(t, "ninja: build stopped") ||
			(strings.HasPrefix(t, "[") && ninjaProgress.MatchString(t)) {
			end = j
			break
		}
	}
	errorLine, msg := -1, ""
	command := i + 1
	for j := command + 1; j < end; j++ {
		if m, ok := buildError(in.Lines[j].Text); ok {
			errorLine, msg = j, m
			break
		}
	}
	excerpt := []Line{in.Lines[i]}
	line := in.Lines[i].N
	from := min(command, end)
	if errorLine >= 0 {
		title += ": " + msg
		line = in.Lines[errorLine].N
		from = errorLine
	}
	excerpt = append(excerpt, in.Lines[from:min(from+outputLines-1, end)]...)
	return &Cause{Title: title, Line: line, Confidence: High, Excerpt: excerpt}
}

func compile(in *Input) *Cause {
	for i, l := range in.Lines {
		var title string
		if m := matchCompilerError(l.Text); m != nil {
			title = "compile error: " + path.Base(m[1]) + ":" + m[2] + ": " + m[3]
		} else if m := matchLinkerError(l.Text); m != nil {
			title = "link error: " + path.Base(m[1]) + ":" + m[2] + ": " + m[3]
		} else if containsAny(linkerMarks...)(l.Text) {
			title = "link error: " + strings.TrimSpace(l.Text)
		} else {
			continue
		}
		return &Cause{Title: title, Line: l.N, Confidence: High, Excerpt: in.span(i, 1, outputLines)}
	}
	return nil
}

const dhCannotFind = `Cannot find (any matches for) "`

func dhInstall(in *Input) *Cause {
	isDh := hasPrefix("dh_install: ", "dh_install:", "dh_missing: ")
	errorLine := in.find(0, func(s string) bool {
		return isDh(s) && (strings.Contains(s, "error:") || strings.HasSuffix(strings.TrimSpace(s), "missing files, aborting"))
	})
	if errorLine < 0 {
		return nil
	}
	tool, _, _ := strings.Cut(strings.TrimSpace(in.Lines[errorLine].Text), ":")

	var items []Line
	var paths []string
	for _, l := range in.Lines[:errorLine] {
		if !isDh(l.Text) {
			continue
		}
		var p string
		if _, rest, ok := strings.Cut(l.Text, dhCannotFind); ok {
			p, _, _ = strings.Cut(rest, `"`)
		} else if _, rest, ok := strings.Cut(l.Text, "warning: "); ok && strings.Contains(rest, " exists in ") {
			p, _, _ = strings.Cut(rest, " exists in ")
		}
		if p != "" {
			items = append(items, l)
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		_, msg, _ := strings.Cut(in.Lines[errorLine].Text, ": ")
		return &Cause{
			Title:      tool + ": " + strings.TrimSpace(msg),
			Line:       in.Lines[errorLine].N,
			Confidence: High,
			Excerpt:    in.span(errorLine, linesBefore, linesAfter),
		}
	}
	what := "not found"
	if tool == "dh_missing" {
		what = "installed by the build but in no package"
	}
	title := fmt.Sprintf("%s: %d %s %s: %s%s", tool, len(paths), plural(len(paths), "file", "files"), what, paths[0], more(len(paths)-1))
	if len(items) > maxExcerptLines-1 {
		items = items[:maxExcerptLines-1]
	}
	return &Cause{
		Title:      title,
		Line:       items[0].N,
		Confidence: High,
		Excerpt:    append(slices.Clone(items), in.Lines[errorLine]),
	}
}

// debhelperError is the error of a debhelper or dpkg tool other than the
// dpkg-buildpackage that runs them.
var debhelperError = regexp.MustCompile(`^(?:dh_[a-z_]+|dpkg-(?:shlibdeps|gencontrol|gensymbols|source|deb|checkbuilddeps)): error: `)

func debhelper(in *Input) *Cause {
	i := in.find(0, func(s string) bool {
		return (strings.HasPrefix(s, "dh_") || strings.HasPrefix(s, "dpkg-")) && debhelperError.MatchString(s)
	})
	if i < 0 {
		return nil
	}
	return &Cause{
		Title:      strings.TrimSpace(in.Lines[i].Text),
		Line:       in.Lines[i].N,
		Confidence: High,
		Excerpt:    in.span(i, linesBefore, linesAfter),
	}
}

// errorWords make a line look like an error to the generic cause.
var errorWords = []string{"error", "err!", "fatal", "failed", "failure", "exception", "cannot ", "not found", "denied", "abort"}

// noErrors is a count of none: "0 errors", "0 tests failed".
var noErrors = regexp.MustCompile(`\b0 (?:[a-z]+ )?(?:errors?|failed|failures?)\b`)

func looksLikeError(s string) bool {
	lower := strings.ToLower(s)
	if strings.Contains(lower, "-werror") || (strings.Contains(lower, "0 ") && noErrors.MatchString(lower)) {
		return false
	}
	return containsAny(errorWords...)(lower)
}

// genericWindow are the last lines the generic cause looks at.
const genericWindow = 200

// generic is the cause when no rule matches: the exit code and the last lines
// that look like errors, or the last lines.
func generic(in *Input) *Cause {
	var excerpt []Line
	for _, l := range in.Lines[max(len(in.Lines)-genericWindow, 0):] {
		if looksLikeError(l.Text) {
			excerpt = append(excerpt, l)
		}
	}
	if len(excerpt) > tailLines {
		excerpt = excerpt[len(excerpt)-tailLines:]
	}
	title := fmt.Sprintf("exit code %d", in.ExitCode)
	if in.ExitCode == 0 {
		title = "the step ended with status " + string(in.State)
	}
	if len(excerpt) > 0 {
		title += ": " + strings.TrimSpace(excerpt[0].Text)
	} else {
		excerpt = in.lastLines()
		if len(excerpt) == 0 {
			title += ", the log is empty"
		}
	}
	return &Cause{Kind: GenericKind, Title: title, Line: lineOf(excerpt), Confidence: Low, Excerpt: excerpt}
}
