package sandbox

import (
	"errors"
	"fmt"
	"io/fs"
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

// walkJailedFiles traverses startRel strictly inside j.osRoot, visiting regular
// files in deterministic lexicographical order. Symlinks are skipped cleanly
// without ever escaping os.Root or entering cycles. Returning errStopWalk
// terminates traversal cleanly.
var errStopWalk = errors.New("stop walk")

func (j *WorkspaceJail) walkJailedFiles(startRel string, fn func(relFromRoot, relFromStart string, info os.FileInfo) error) error {
	if startRel == "" {
		startRel = "."
	}
	if !fs.ValidPath(startRel) {
		return fmt.Errorf("%w: invalid walk root", ErrPathEscape)
	}

	var walk func(dirRel, relPrefix string) error
	walk = func(dirRel, relPrefix string) error {
		dirFile, err := j.osRoot.Open(dirRel)
		if err != nil {
			return nil
		}
		entries, readErr := dirFile.ReadDir(-1)
		_ = dirFile.Close()
		if readErr != nil {
			return nil
		}
		sort.Slice(entries, func(a, b int) bool {
			return entries[a].Name() < entries[b].Name()
		})

		for _, entry := range entries {
			name := entry.Name()
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
				continue
			}
			childRel := name
			if dirRel != "." {
				childRel = dirRel + "/" + name
			}
			childFromStart := name
			if relPrefix != "" {
				childFromStart = relPrefix + "/" + name
			}

			// Skip symlinks strictly (both via DirEntry type and os.Root.Lstat).
			if entry.Type()&fs.ModeSymlink != 0 {
				continue
			}
			linfo, lerr := j.osRoot.Lstat(childRel)
			if lerr != nil {
				continue
			}
			if linfo.Mode()&os.ModeSymlink != 0 {
				continue
			}

			if linfo.IsDir() {
				if err := walk(childRel, childFromStart); err != nil {
					return err
				}
				continue
			}
			if linfo.Mode().IsRegular() {
				if err := fn(childRel, childFromStart, linfo); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := walk(startRel, "")
	if errors.Is(err, errStopWalk) {
		return nil
	}
	return err
}
