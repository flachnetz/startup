package fsx

import "io/fs"

type NoReadDirFS struct {
	f fs.FS
}

func (n NoReadDirFS) Open(name string) (fs.File, error) {
	file, err := n.f.Open(name)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, fs.ErrPermission
	}

	return file, nil
}
