package sandbox

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// matchGlobstar reports whether slash-separated relPath matches pattern,
// supporting ** (zero or more directory segments), *, ?, and [...] syntax.
func matchGlobstar(pattern, relPath string) (bool, error) {
	pattern = strings.TrimPrefix(filepath.ToSlash(pattern), "./")
	relPath = strings.TrimPrefix(filepath.ToSlash(relPath), "./")
	if pattern == "" {
		return false, errors.New("glob pattern is empty")
	}
	if strings.ContainsRune(pattern, 0) {
		return false, errors.New("glob pattern contains NUL byte")
	}

	patSegs := strings.Split(pattern, "/")
	pathSegs := strings.Split(relPath, "/")

	// Validate each non-globstar segment upfront so malformed syntax (like "[")
	// fails deterministically even when early segments don't match.
	for _, seg := range patSegs {
		if seg == "**" {
			continue
		}
		if _, err := path.Match(seg, "a"); err != nil {
			return false, fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
		}
	}

	// Memoized DP over (patIdx, pathIdx).
	pLen := len(patSegs)
	sLen := len(pathSegs)
	memo := make([]int8, (pLen+1)*(sLen+1))

	var match func(pi, si int) bool
	match = func(pi, si int) bool {
		key := pi*(sLen+1) + si
		if v := memo[key]; v != 0 {
			return v == 1
		}
		var ok bool
		switch {
		case pi == pLen && si == sLen:
			ok = true
		case pi == pLen:
			ok = false
		case patSegs[pi] == "**":
			// ** matches zero segments OR consumes one segment and stays on **.
			ok = match(pi+1, si) || (si < sLen && match(pi, si+1))
		case si == sLen:
			ok = false
		default:
			matched, _ := path.Match(patSegs[pi], pathSegs[si])
			ok = matched && match(pi+1, si+1)
		}
		if ok {
			memo[key] = 1
		} else {
			memo[key] = -1
		}
		return ok
	}
	return match(0, 0), nil
}

// walkJailedFiles traverses baseDir strictly inside jail, visiting regular
// files in deterministic lexicographical order. Symlinks escaping jail.Root()
// or broken symlinks are skipped; in-workspace directory symlinks are guarded
// against cycles. Returning errStopWalk terminates traversal cleanly.
var errStopWalk = errors.New("stop walk")

func (j *WorkspaceJail) walkJailedFiles(baseDir string, fn func(absPath, relPath string, info os.FileInfo) error) error {
	visitedDirs := make(map[string]bool)

	var walk func(realDir, relPrefix string) error
	walk = func(realDir, relPrefix string) error {
		if !isWithinRoot(j.root, realDir) {
			return nil
		}
		if visitedDirs[realDir] {
			return nil
		}
		visitedDirs[realDir] = true

		entries, err := os.ReadDir(realDir)
		if err != nil {
			return nil
		}
		sort.Slice(entries, func(a, b int) bool {
			return entries[a].Name() < entries[b].Name()
		})

		for _, entry := range entries {
			name := entry.Name()
			childPath := filepath.Join(realDir, name)
			childRel := name
			if relPrefix != "" {
				childRel = relPrefix + "/" + name
			}

			linfo, err := os.Lstat(childPath)
			if err != nil {
				continue
			}

			if linfo.Mode()&os.ModeSymlink != 0 {
				resolved, evalErr := filepath.EvalSymlinks(childPath)
				if evalErr != nil {
					// Skip broken symlinks.
					continue
				}
				resolved = filepath.Clean(resolved)
				if !isWithinRoot(j.root, resolved) {
					// Skip symlinks escaping workspace jail!
					continue
				}
				rinfo, statErr := os.Stat(resolved)
				if statErr != nil {
					continue
				}
				if rinfo.IsDir() {
					if err := walk(resolved, childRel); err != nil {
						return err
					}
					continue
				}
				if rinfo.Mode().IsRegular() {
					if err := fn(resolved, childRel, rinfo); err != nil {
						return err
					}
				}
				continue
			}

			if linfo.IsDir() {
				if err := walk(childPath, childRel); err != nil {
					return err
				}
				continue
			}
			if linfo.Mode().IsRegular() {
				if err := fn(childPath, childRel, linfo); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := walk(baseDir, "")
	if errors.Is(err, errStopWalk) {
		return nil
	}
	return err
}
