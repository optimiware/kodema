package sandbox

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// ErrPathEscape indicates a path attempts to leave the jailed workspace root
// via absolute path, parent traversal, or symlink escape.
var ErrPathEscape = errors.New("path escapes workspace jail")

// WorkspaceJail confines all file operations strictly inside a canonicalized
// workspace root directory.
type WorkspaceJail struct {
	root     string
	origRoot string
}

// NewWorkspaceJail canonicalizes dir with filepath.Abs and filepath.EvalSymlinks
// and verifies it exists and is a directory.
func NewWorkspaceJail(dir string) (*WorkspaceJail, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return nil, errors.New("workspace directory is empty")
	}
	if strings.ContainsRune(trimmed, 0) {
		return nil, errors.New("workspace directory contains NUL byte")
	}
	absDir, err := filepath.Abs(filepath.Clean(trimmed))
	if err != nil {
		return nil, fmt.Errorf("resolve workspace directory: %w", err)
	}
	realDir, err := filepath.EvalSymlinks(absDir)
	if err != nil {
		return nil, fmt.Errorf("canonicalize workspace directory %q: %w", absDir, err)
	}
	info, err := os.Stat(realDir)
	if err != nil {
		return nil, fmt.Errorf("stat workspace directory %q: %w", realDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("workspace path %q is not a directory", realDir)
	}
	return &WorkspaceJail{
		root:     filepath.Clean(realDir),
		origRoot: filepath.Clean(absDir),
	}, nil
}

// Root returns the canonical workspace root path.
func (j *WorkspaceJail) Root() string {
	return j.root
}

// Rel returns the workspace-relative slash path for an already-jailed path.
func (j *WorkspaceJail) Rel(resolved string) string {
	rel, err := filepath.Rel(j.root, resolved)
	if err != nil {
		return resolved
	}
	return filepath.ToSlash(rel)
}

// ResolveFile resolves a non-empty file path strictly inside the workspace.
// When allowMissing is false, the target must exist and must not traverse or
// terminate in a symlink that escapes the workspace.
func (j *WorkspaceJail) ResolveFile(rawPath string, allowMissing bool) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		return "", errors.New("path is required")
	}
	return j.resolveJailedPath(rawPath, allowMissing, false)
}

// ResolveForWrite resolves a target path for writing or editing. It refuses
// empty paths, workspace root itself, NUL bytes, traversal, escaping ancestor
// symlinks, and any symlink (valid or broken) at the final target path.
func (j *WorkspaceJail) ResolveForWrite(rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		return "", errors.New("path is required")
	}
	resolved, err := j.resolveJailedPath(rawPath, true, true)
	if err != nil {
		return "", err
	}
	if resolved == j.root {
		return "", errors.New("cannot write to workspace root directory")
	}
	return resolved, nil
}

// ResolveDir resolves an optional directory path (empty or "." means workspace
// root) strictly inside the workspace and verifies it is a directory.
func (j *WorkspaceJail) ResolveDir(rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" || strings.TrimSpace(rawPath) == "." {
		return j.root, nil
	}
	resolved, err := j.resolveJailedPath(rawPath, false, false)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("path %q is not a directory", rawPath)
	}
	return resolved, nil
}

// ResolveExisting resolves an optional path (empty or "." means workspace root)
// to an existing file or directory strictly inside the workspace.
func (j *WorkspaceJail) ResolveExisting(rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" || strings.TrimSpace(rawPath) == "." {
		return j.root, nil
	}
	return j.resolveJailedPath(rawPath, false, false)
}

// resolveJailedPath validates rawPath against NUL bytes, traversal, outside
// absolute paths, and symlink escapes by walking every path component and
// verifying EvalSymlinks containment on the deepest existing ancestor and
// target itself.
func (j *WorkspaceJail) resolveJailedPath(rawPath string, allowMissing bool, refuseTargetSymlink bool) (string, error) {
	if strings.ContainsRune(rawPath, 0) {
		return "", fmt.Errorf("%w: path contains NUL byte", ErrPathEscape)
	}
	if rawPath == "" {
		return "", errors.New("path is empty")
	}

	relInput, err := j.toRelativeInput(rawPath)
	if err != nil {
		return "", err
	}
	if relInput == "" || relInput == "." {
		return j.root, nil
	}

	// Split raw relative path without collapsing symlinks first, so sequences
	// like "symlink_to_etc/../passwd" cannot hide an escaping component.
	rawSegments := strings.Split(filepath.ToSlash(relInput), "/")
	cur := j.root
	for idx, seg := range rawSegments {
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			// Verify cur before popping, and refuse popping above workspace root.
			if cur == j.root || !isWithinRoot(j.root, cur) {
				return "", fmt.Errorf("%w: traversal outside workspace", ErrPathEscape)
			}
			cur = filepath.Dir(cur)
			if !isWithinRoot(j.root, cur) {
				return "", fmt.Errorf("%w: traversal outside workspace", ErrPathEscape)
			}
			continue
		}

		next := filepath.Join(cur, seg)
		if !isWithinRoot(j.root, next) {
			return "", fmt.Errorf("%w: path leaves workspace root", ErrPathEscape)
		}

		info, lstatErr := os.Lstat(next)
		if lstatErr != nil {
			if !os.IsNotExist(lstatErr) {
				return "", fmt.Errorf("inspect path component: %w", lstatErr)
			}
			if !allowMissing {
				return "", os.ErrNotExist
			}
			// Deepest existing ancestor is cur; verify its canonical path is
			// strictly contained inside j.root.
			resolvedCur, evalErr := filepath.EvalSymlinks(cur)
			if evalErr != nil {
				return "", fmt.Errorf("%w: cannot verify ancestor directory: %v", ErrPathEscape, evalErr)
			}
			resolvedCur = filepath.Clean(resolvedCur)
			if !isWithinRoot(j.root, resolvedCur) {
				return "", fmt.Errorf("%w: ancestor symlink escapes workspace", ErrPathEscape)
			}
			// Process remaining non-existent segments strictly without leaving j.root.
			out := resolvedCur
			for _, rem := range rawSegments[idx:] {
				if rem == "" || rem == "." {
					continue
				}
				if rem == ".." {
					if out == j.root || !isWithinRoot(j.root, out) {
						return "", fmt.Errorf("%w: traversal outside workspace", ErrPathEscape)
					}
					out = filepath.Dir(out)
					if !isWithinRoot(j.root, out) {
						return "", fmt.Errorf("%w: traversal outside workspace", ErrPathEscape)
					}
					continue
				}
				out = filepath.Join(out, rem)
				if !isWithinRoot(j.root, out) {
					return "", fmt.Errorf("%w: path leaves workspace root", ErrPathEscape)
				}
			}
			return out, nil
		}

		isLast := isLastNonTrivialSegment(rawSegments[idx+1:])
		if info.Mode()&os.ModeSymlink != 0 {
			if refuseTargetSymlink && isLast {
				return "", fmt.Errorf("%w: refusing to write through symlink", ErrPathEscape)
			}
			resolvedNext, evalErr := filepath.EvalSymlinks(next)
			if evalErr != nil {
				// Broken symlink (could point to /etc/nonexistent or /kodema/secrets/new).
				return "", fmt.Errorf("%w: broken or unreadable symlink", ErrPathEscape)
			}
			resolvedNext = filepath.Clean(resolvedNext)
			if !isWithinRoot(j.root, resolvedNext) {
				return "", fmt.Errorf("%w: symlink target escapes workspace", ErrPathEscape)
			}
			cur = resolvedNext
			continue
		}
		cur = next
	}

	// Final canonical check on existing target.
	resolvedFinal, err := filepath.EvalSymlinks(cur)
	if err != nil {
		return "", fmt.Errorf("%w: cannot canonicalize target: %v", ErrPathEscape, err)
	}
	resolvedFinal = filepath.Clean(resolvedFinal)
	if !isWithinRoot(j.root, resolvedFinal) {
		return "", fmt.Errorf("%w: target escapes workspace", ErrPathEscape)
	}
	return resolvedFinal, nil
}

func (j *WorkspaceJail) toRelativeInput(rawPath string) (string, error) {
	if !filepath.IsAbs(rawPath) {
		return rawPath, nil
	}
	// Strip either canonical root or original (pre-EvalSymlinks) root prefix
	// without collapsing ".." segments first, so symlink escapes inside an
	// absolute workspace path are still caught component by component.
	slashRaw := filepath.ToSlash(rawPath)
	for _, base := range []string{filepath.ToSlash(j.root), filepath.ToSlash(j.origRoot)} {
		if base == "" {
			continue
		}
		trimmedBase := strings.TrimRight(base, "/")
		if slashRaw == trimmedBase {
			return ".", nil
		}
		if strings.HasPrefix(slashRaw, trimmedBase+"/") {
			return strings.TrimPrefix(slashRaw, trimmedBase+"/"), nil
		}
	}
	return "", fmt.Errorf("%w: absolute path %q is outside workspace", ErrPathEscape, rawPath)
}

func isLastNonTrivialSegment(rest []string) bool {
	for _, seg := range rest {
		if seg != "" && seg != "." {
			return false
		}
	}
	return true
}

// isWithinRoot returns true iff candidate equals root or is strictly below root.
func isWithinRoot(root, candidate string) bool {
	root = filepath.Clean(root)
	candidate = filepath.Clean(candidate)
	if candidate == root {
		return true
	}
	prefix := root
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(candidate, prefix)
}

// SafeOpenRead opens an already-jailed canonical regular file with O_NOFOLLOW
// and verifies it is a regular file within the workspace.
func (j *WorkspaceJail) SafeOpenRead(canonicalPath string) (*os.File, os.FileInfo, error) {
	if !isWithinRoot(j.root, canonicalPath) {
		return nil, nil, fmt.Errorf("%w: path outside workspace", ErrPathEscape)
	}
	f, err := os.OpenFile(canonicalPath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, nil, fmt.Errorf("%w: refused symlink on open", ErrPathEscape)
		}
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, errors.New("target is not a regular file")
	}
	return f, info, nil
}

// EnsureParentDirs creates missing parent directories strictly inside the
// workspace jail, verifying every existing component is a real directory
// (never crossing a symlink escaping the workspace).
func (j *WorkspaceJail) EnsureParentDirs(targetPath string) (string, error) {
	if !isWithinRoot(j.root, targetPath) {
		return "", fmt.Errorf("%w: target outside workspace", ErrPathEscape)
	}
	parent := filepath.Dir(targetPath)
	rel, err := filepath.Rel(j.root, parent)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: parent outside workspace", ErrPathEscape)
	}
	if rel == "." {
		return j.root, nil
	}
	cur := j.root
	for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			return "", fmt.Errorf("%w: traversal in parent", ErrPathEscape)
		}
		next := filepath.Join(cur, seg)
		info, lstatErr := os.Lstat(next)
		if lstatErr != nil {
			if !os.IsNotExist(lstatErr) {
				return "", lstatErr
			}
			if err := os.Mkdir(next, 0o755); err != nil && !os.IsExist(err) {
				return "", fmt.Errorf("create directory %q: %w", seg, err)
			}
			cur = next
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, evalErr := filepath.EvalSymlinks(next)
			if evalErr != nil || !isWithinRoot(j.root, resolved) {
				return "", fmt.Errorf("%w: parent symlink escapes workspace", ErrPathEscape)
			}
			rInfo, statErr := os.Stat(resolved)
			if statErr != nil || !rInfo.IsDir() {
				return "", errors.New("parent path is not a directory")
			}
			cur = resolved
			continue
		}
		if !info.IsDir() {
			return "", fmt.Errorf("parent component %q is not a directory", seg)
		}
		cur = next
	}
	resolvedParent, err := filepath.EvalSymlinks(cur)
	if err != nil || !isWithinRoot(j.root, resolvedParent) {
		return "", fmt.Errorf("%w: verified parent escapes workspace", ErrPathEscape)
	}
	return filepath.Clean(resolvedParent), nil
}

// AtomicWrite writes data atomically to canonicalTarget via a temp file in the
// verified canonical parent directory and rename. Refuses if the target is a
// symlink or directory.
func (j *WorkspaceJail) AtomicWrite(canonicalTarget string, data []byte) error {
	parentDir, err := j.EnsureParentDirs(canonicalTarget)
	if err != nil {
		return err
	}
	base := filepath.Base(canonicalTarget)
	if base == "" || base == "." || base == ".." || strings.ContainsRune(base, filepath.Separator) {
		return fmt.Errorf("%w: invalid target filename", ErrPathEscape)
	}
	finalTarget := filepath.Join(parentDir, base)
	if !isWithinRoot(j.root, finalTarget) {
		return fmt.Errorf("%w: target outside workspace", ErrPathEscape)
	}

	mode := os.FileMode(0o644)
	if info, err := os.Lstat(finalTarget); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: refusing to overwrite symlink target", ErrPathEscape)
		}
		if info.IsDir() {
			return errors.New("cannot write to a directory")
		}
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}

	tmp, err := os.CreateTemp(parentDir, ".kodema-write-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Re-check target immediately before rename to narrow TOCTOU window.
	if info, err := os.Lstat(finalTarget); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: target became a symlink before rename", ErrPathEscape)
		}
		if info.IsDir() {
			return errors.New("target became a directory")
		}
	}

	if err := os.Rename(tmpName, finalTarget); err != nil {
		return fmt.Errorf("install written file: %w", err)
	}
	cleanup = false
	return nil
}

// ReadBoundedRegular reads up to maxBytes+1 from an already-open regular file
// and returns an error if the file exceeds maxBytes.
func ReadBoundedRegular(f *os.File, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("file exceeds maximum allowed size of %d bytes", maxBytes)
	}
	return data, nil
}
