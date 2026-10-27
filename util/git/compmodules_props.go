package git

import "fmt"

// RenameModule gives a module another name or order, its entry becoming
// S<order>-<name>, in one commit made from the revision base. The name of
// another module is refused, whatever its order: an agent finding two refuses
// to run either. It returns the module as renamed.
func (m CompModules) RenameModule(name, base, newName string, newOrder int, author, message string) (*ModuleDetail, error) {
	if !ValidModuleName(newName) {
		return nil, invalidf("invalid module name %q", newName)
	}
	if newOrder < 0 || newOrder > MaxModuleOrder {
		return nil, invalidf("invalid module order %d", newOrder)
	}
	if err := m.renameLocked(name, base, newName, newOrder, author, message); err != nil {
		return nil, err
	}
	return m.Get(newName)
}

func (m CompModules) renameLocked(name, base, newName string, newOrder int, author, message string) error {
	compModulesWrite.Lock()
	defer compModulesWrite.Unlock()
	head, err := m.head()
	if err != nil {
		return err
	}
	if head == "" {
		return ErrModuleNotFound
	}
	mods, err := m.find(head, name)
	if err != nil {
		return err
	}
	if len(mods) == 0 {
		return ErrModuleNotFound
	}
	if len(mods) > 1 {
		return invalidf("%d modules are named %s: the agents refuse to run either", len(mods), name)
	}
	mod := mods[0]
	rev, err := m.revision(head, mod.Path)
	if err != nil {
		return err
	}
	if base != rev {
		return ErrModuleStale
	}
	entry := fmt.Sprintf("S%02d-%s", newOrder, newName)
	if entry == mod.Path {
		return nil
	}
	if newName != name {
		others, err := m.find(head, newName)
		if err != nil {
			return err
		}
		if len(others) > 0 {
			return ErrModuleExists
		}
	}
	files, paths, err := m.moduleFiles(head, mod)
	if err != nil {
		return err
	}
	return m.commit(head, treeFiles(entry, files), paths, author, message)
}

// SetExecutable makes a file of a directory module executable, or not, in one
// commit made from the revision base: a binary file, say, which the editor
// does not open. The entry stays executable.
func (m CompModules) SetExecutable(name, base, rel string, executable bool, author, message string) (*ModuleDetail, error) {
	return m.rewrite(name, base, author, message, func(_ Module, files map[string]ModuleBlob) (string, map[string]ModuleBlob, error) {
		if kindOf(files) == ModuleKindScript {
			return "", nil, invalidf("the script of a script module stays executable")
		}
		f, ok := files[rel]
		if !ok {
			return "", nil, ErrModuleNotFound
		}
		next := copyFiles(files)
		f.Executable = executable
		next[rel] = f
		return ModuleKindDirectory, next, nil
	})
}
