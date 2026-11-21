package sandbox

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
)

// ErrPathEscape indicates a path attempts to leave the jailed workspace root
// via absolute path, parent traversal, or symlink escape.
var ErrPathEscape = errors.New("path escapes workspace jail")

// WorkspaceJail confines all file operations strictly inside a canonicalized
// workspace root directory backed by Go's kernel-enforced os.Root
// (openat2 RESOLVE_BENEATH on Linux).
type WorkspaceJail struct {
	root     string
	origRoot string
	osRoot   *os.Root
}

// NewWorkspaceJail canonicalizes dir with filepath.Abs and filepath.EvalSymlinks
// and opens an os.Root handle enforcing kernel-level containment beneath it.
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
	osRoot, err := os.OpenRoot(realDir)
	if err != nil {
		return nil, fmt.Errorf("open workspace root %q: %w", realDir, err)
	}
	info, err := osRoot.Stat(".")
	if err != nil {
		_ = osRoot.Close()
		return nil, fmt.Errorf("stat workspace directory %q: %w", realDir, err)
	}
	if !info.IsDir() {
		_ = osRoot.Close()
		return nil, fmt.Errorf("workspace path %q is not a directory", realDir)
	}
	return &WorkspaceJail{
		root:     filepath.Clean(realDir),
		origRoot: filepath.Clean(absDir),
		osRoot:   osRoot,
	}, nil
}

// Close releases the underlying os.Root file descriptor.
func (j *WorkspaceJail) Close() error {
	if j == nil || j.osRoot == nil {
		return nil
	}
	return j.osRoot.Close()
}

// Root returns the canonical workspace root path.
func (j *WorkspaceJail) Root() string {
	return j.root
}

// NormalizeRelPath validates rawPath and converts it to a clean slash-separated
// relative path suitable for os.Root operations. It strictly rejects NUL bytes,
// backslashes, outside absolute paths, and any ".." traversal segment (never
// lexically popping ".." across potentially non-existent or symlinked segments).
func (j *WorkspaceJail) NormalizeRelPath(rawPath string, allowRoot bool) (string, error) {
	if strings.ContainsRune(rawPath, 0) {
		return "", fmt.Errorf("%w: path contains NUL byte", ErrPathEscape)
	}
	if strings.TrimSpace(rawPath) == "" {
		if allowRoot {
			return ".", nil
		}
		return "", errors.New("path is required")
	}
	if strings.ContainsRune(rawPath, '\\') {
		return "", fmt.Errorf("%w: path contains invalid separator", ErrPathEscape)
	}

	relInput, err := j.toRelativeInput(rawPath)
	if err != nil {
		return "", err
	}
	if relInput == "" || relInput == "." {
		if allowRoot {
			return ".", nil
		}
		return "", errors.New("cannot target workspace root directory")
	}

	rawSegments := strings.Split(relInput, "/")
	parts := make([]string, 0, len(rawSegments))
	for _, seg := range rawSegments {
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			return "", fmt.Errorf("%w: traversal outside workspace", ErrPathEscape)
		}
		parts = append(parts, seg)
	}
	if len(parts) == 0 {
		if allowRoot {
			return ".", nil
		}
		return "", errors.New("cannot target workspace root directory")
	}

	rel := strings.Join(parts, "/")
	if !fs.ValidPath(rel) {
		return "", fmt.Errorf("%w: invalid relative path", ErrPathEscape)
	}
	return rel, nil
}

func (j *WorkspaceJail) toRelativeInput(rawPath string) (string, error) {
	if !filepath.IsAbs(rawPath) {
		return rawPath, nil
	}
	// Strip either canonical root or original (pre-EvalSymlinks) root prefix
	// without collapsing ".." segments first, so traversal inside an absolute
	// workspace path is always rejected.
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

func wrapRootErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrPathEscape) {
		return err
	}
	if errors.Is(err, syscall.ELOOP) || strings.Contains(err.Error(), "escapes from parent") {
		return fmt.Errorf("%w: %v", ErrPathEscape, err)
	}
	return err
}

// OpenRead opens an existing regular file strictly beneath the os.Root jail.
// When refuseTargetSymlink is true, symlinks at the final path component are
// explicitly rejected with ErrPathEscape (enforced via Lstat + O_NOFOLLOW).
func (j *WorkspaceJail) OpenRead(rawPath string, refuseTargetSymlink bool) (*os.File, os.FileInfo, string, error) {
	rel, err := j.NormalizeRelPath(rawPath, false)
	if err != nil {
		return nil, nil, "", err
	}
	f, info, err := j.openReadRel(rel, refuseTargetSymlink)
	if err != nil {
		return nil, nil, "", err
	}
	return f, info, rel, nil
}

func (j *WorkspaceJail) openReadRel(rel string, refuseTargetSymlink bool) (*os.File, os.FileInfo, error) {
	if !fs.ValidPath(rel) || rel == "." {
		return nil, nil, fmt.Errorf("%w: invalid relative file path", ErrPathEscape)
	}
	var (
		f   *os.File
		err error
	)
	if refuseTargetSymlink {
		linfo, lerr := j.osRoot.Lstat(rel)
		if lerr != nil {
			return nil, nil, wrapRootErr(lerr)
		}
		if linfo.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fmt.Errorf("%w: refusing to open symlink target", ErrPathEscape)
		}
		f, err = j.osRoot.OpenFile(rel, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	} else {
		f, err = j.osRoot.Open(rel)
	}
	if err != nil {
		return nil, nil, wrapRootErr(err)
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

// StatExisting validates rawPath (empty or "." means workspace root) and stats
// it strictly through os.Root.
func (j *WorkspaceJail) StatExisting(rawPath string) (string, os.FileInfo, error) {
	rel, err := j.NormalizeRelPath(rawPath, true)
	if err != nil {
		return "", nil, err
	}
	info, err := j.osRoot.Stat(rel)
	if err != nil {
		return "", nil, wrapRootErr(err)
	}
	return rel, info, nil
}

// ResolveDirRel resolves an optional directory path (empty or "." means
// workspace root) strictly through os.Root and verifies it is a directory.
func (j *WorkspaceJail) ResolveDirRel(rawPath string) (string, error) {
	rel, info, err := j.StatExisting(rawPath)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("path %q is not a directory", rawPath)
	}
	return rel, nil
}

// ResolveWorkdir validates an optional working directory for child command
// execution through os.Root and returns its host directory path beneath j.root.
func (j *WorkspaceJail) ResolveWorkdir(rawPath string) (string, error) {
	rel, err := j.ResolveDirRel(rawPath)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return j.root, nil
	}
	return filepath.Join(j.root, filepath.FromSlash(rel)), nil
}

// AtomicWrite validates rawPath and writes data atomically inside os.Root via
// a temp file in the target's parent directory and rename. Refuses if the
// target is a symlink, directory, or workspace root.
func (j *WorkspaceJail) AtomicWrite(rawPath string, data []byte) (string, error) {
	rel, err := j.NormalizeRelPath(rawPath, false)
	if err != nil {
		return "", err
	}
	if err := j.AtomicWriteRel(rel, data); err != nil {
		return "", err
	}
	return rel, nil
}

// AtomicWriteRel writes data atomically to an already-normalized relative path
// strictly inside os.Root.
func (j *WorkspaceJail) AtomicWriteRel(rel string, data []byte) error {
	if !fs.ValidPath(rel) || rel == "." {
		return fmt.Errorf("%w: invalid target path", ErrPathEscape)
	}
	base := path.Base(rel)
	if base == "" || base == "." || base == ".." {
		return fmt.Errorf("%w: invalid target filename", ErrPathEscape)
	}
	parent := path.Dir(rel)
	if parent != "." {
		if err := j.osRoot.MkdirAll(parent, 0o755); err != nil {
			return wrapRootErr(fmt.Errorf("create parent directories: %w", err))
		}
	}

	mode := os.FileMode(0o644)
	if info, err := j.osRoot.Lstat(rel); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: refusing to overwrite symlink target", ErrPathEscape)
		}
		if info.IsDir() {
			return errors.New("cannot write to a directory")
		}
		mode = info.Mode().Perm()
	} else if !errors.Is(err, os.ErrNotExist) {
		return wrapRootErr(err)
	}

	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Errorf("generate temp filename: %w", err)
	}
	tmpBase := ".kodema-write-" + hex.EncodeToString(suffix[:])
	tmpRel := tmpBase
	if parent != "." {
		tmpRel = parent + "/" + tmpBase
	}

	tmp, err := j.osRoot.OpenFile(tmpRel, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return wrapRootErr(fmt.Errorf("create temp file: %w", err))
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = j.osRoot.Remove(tmpRel)
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

	// Re-check target immediately before rename via os.Root.Lstat.
	if info, err := j.osRoot.Lstat(rel); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: target became a symlink before rename", ErrPathEscape)
		}
		if info.IsDir() {
			return errors.New("target became a directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return wrapRootErr(err)
	}

	if err := j.osRoot.Rename(tmpRel, rel); err != nil {
		return wrapRootErr(fmt.Errorf("install written file: %w", err))
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
