package git

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"path"
	"strings"
)

// MaxModuleArchiveSize is the largest archive of a module read, in bytes.
const MaxModuleArchiveSize = 20 << 20

// ParseModuleArchive reads the files of a directory module from a gzipped
// tarball or a zip archive, by path relative to the module.
//
// Only regular files are taken, directories implied: a link, a device or any
// other entry is refused, as is a path out of the module. A tarball keeps the
// executable bits of its files; a zip, made without them on Windows say,
// marks executable the files starting with #! when it carries no Unix mode.
// An archive holding one directory at its root, the module exported or the
// directory it was made from, is read from inside it.
func ParseModuleArchive(data []byte) (map[string]ModuleBlob, error) {
	if len(data) > MaxModuleArchiveSize {
		return nil, invalidf("the archive is larger than %d bytes", MaxModuleArchiveSize)
	}
	var files map[string]ModuleBlob
	var err error
	switch {
	case bytes.HasPrefix(data, []byte{0x1f, 0x8b}):
		files, err = parseTarGz(data)
	case bytes.HasPrefix(data, []byte("PK\x03\x04")):
		files, err = parseZip(data)
	default:
		return nil, invalidf("not a .tar.gz or .zip archive")
	}
	if err != nil {
		return nil, err
	}
	return unwrapRoot(files), nil
}

// archivePath cleans the path of an entry: no leading ./ nor trailing /.
func archivePath(name string) (string, error) {
	p := strings.TrimPrefix(strings.TrimSuffix(name, "/"), "./")
	if p == "" || strings.HasPrefix(p, "/") || path.Clean(p) != p || strings.HasPrefix(p, "../") || p == ".." {
		return "", invalidf("the archive holds a path out of the module: %q", name)
	}
	return p, nil
}

func readLimited(r io.Reader, name string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, MaxModuleFileSize+1))
	if err != nil {
		return nil, invalidf("cannot read %s: %v", name, err)
	}
	if len(b) > MaxModuleFileSize {
		return nil, invalidf("%s is larger than %d bytes", name, MaxModuleFileSize)
	}
	return b, nil
}

func addFile(files map[string]ModuleBlob, p string, f ModuleBlob) error {
	if _, ok := files[p]; ok {
		return invalidf("the archive holds %s twice", p)
	}
	if len(files) >= MaxModuleFiles+1 {
		return invalidf("the archive holds more than %d files", MaxModuleFiles)
	}
	files[p] = f
	return nil
}

func parseTarGz(data []byte) (map[string]ModuleBlob, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, invalidf("not a gzipped tarball: %v", err)
	}
	tr := tar.NewReader(io.LimitReader(gz, MaxModuleSize+MaxModuleArchiveSize))
	files := map[string]ModuleBlob{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, invalidf("cannot read the tarball: %v", err)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
		case tar.TypeXGlobalHeader:
			continue
		default:
			return nil, invalidf("%s is not a regular file: links and devices are refused", h.Name)
		}
		p, err := archivePath(h.Name)
		if err != nil {
			return nil, err
		}
		b, err := readLimited(tr, p)
		if err != nil {
			return nil, err
		}
		if err := addFile(files, p, ModuleBlob{Content: b, Executable: h.Mode&0o111 != 0}); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func parseZip(data []byte) (map[string]ModuleBlob, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, invalidf("not a zip archive: %v", err)
	}
	files := map[string]ModuleBlob{}
	for _, f := range zr.File {
		mode := f.Mode()
		if mode.IsDir() {
			continue
		}
		if !mode.IsRegular() {
			return nil, invalidf("%s is not a regular file: links and devices are refused", f.Name)
		}
		p, err := archivePath(f.Name)
		if err != nil {
			return nil, err
		}
		if f.UncompressedSize64 > MaxModuleFileSize {
			return nil, invalidf("%s is larger than %d bytes", p, MaxModuleFileSize)
		}
		rc, err := f.Open()
		if err != nil {
			return nil, invalidf("cannot read %s: %v", p, err)
		}
		b, err := readLimited(rc, p)
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		// Made on Unix, the archive carries the modes in the high bits of the
		// external attributes; otherwise a script tells itself by its #!.
		executable := mode&0o111 != 0
		if f.CreatorVersion>>8 != 3 {
			executable = bytes.HasPrefix(b, []byte("#!"))
		}
		if err := addFile(files, p, ModuleBlob{Content: b, Executable: executable}); err != nil {
			return nil, err
		}
	}
	return files, nil
}

// unwrapRoot reads an archive holding one directory at its root from inside
// it, unless that directory is the scripts/ of the module.
func unwrapRoot(files map[string]ModuleBlob) map[string]ModuleBlob {
	root := ""
	for p := range files {
		top, _, nested := strings.Cut(p, "/")
		if !nested {
			return files
		}
		if root == "" {
			root = top
		} else if top != root {
			return files
		}
	}
	if root == "" || root == "scripts" {
		return files
	}
	out := make(map[string]ModuleBlob, len(files))
	for p, f := range files {
		out[strings.TrimPrefix(p, root+"/")] = f
	}
	return out
}
