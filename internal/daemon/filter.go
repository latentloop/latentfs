package daemon

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	ignore "github.com/sabhiram/go-gitignore"

	"latentfs/internal/storage"
)

// BuildFileFilter creates a FileFilter function that:
// 1. Always excludes .latentfs directory (hardcoded)
// 2. Checks excludes list (force-exclude, highest priority)
// 3. Checks includes list (force-include, overrides gitignore)
// 4. Applies gitignore rules
func BuildFileFilter(projectDir string, gitignoreEnabled bool, includes, excludes []string) storage.FileFilter {
	var matcher *gitignoreMatcher
	if gitignoreEnabled {
		var err error
		matcher, err = newGitignoreMatcher(projectDir)
		if err != nil {
			log.Printf("filter: failed to build gitignore matcher: %v", err)
		}
	}

	return func(relPath string, isDir bool) bool {
		// Always exclude .latentfs directory
		if relPath == ".latentfs" || strings.HasPrefix(relPath, ".latentfs/") || strings.HasPrefix(relPath, ".latentfs"+string(filepath.Separator)) {
			return false
		}

		// Check excludes (force-exclude, takes precedence over includes)
		for _, exc := range excludes {
			if relPath == exc || strings.HasPrefix(relPath, exc+"/") || strings.HasPrefix(relPath, exc+string(filepath.Separator)) {
				return false
			}
		}

		// Check includes override (force-include even if gitignored)
		for _, inc := range includes {
			if relPath == inc || strings.HasPrefix(relPath, inc+"/") || strings.HasPrefix(relPath, inc+string(filepath.Separator)) {
				return true
			}
		}

		// Apply gitignore rules
		if matcher != nil && matcher.isIgnored(relPath, isDir) {
			return false
		}

		return true
	}
}

// BuildFileFilterFromConfig creates a FileFilter from a ProjectConfig.
// If cfg is nil, returns nil (no filtering).
func BuildFileFilterFromConfig(projectDir string, cfg *ProjectConfig) storage.FileFilter {
	if cfg == nil {
		return nil
	}
	return BuildFileFilter(projectDir, cfg.GitignoreEnabled(), cfg.Includes, cfg.Excludes)
}

// gitignoreMatcher collects .gitignore rules from a project tree
type gitignoreMatcher struct {
	matchers []scopedMatcher
}

type scopedMatcher struct {
	dirPrefix string
	ignore    *ignore.GitIgnore
}

func newGitignoreMatcher(projectDir string) (*gitignoreMatcher, error) {
	m := &gitignoreMatcher{}

	err := filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == ".git" && path != projectDir {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Base(path) != ".gitignore" {
			return nil
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}

		dir := filepath.Dir(path)
		relDir, relErr := filepath.Rel(projectDir, dir)
		if relErr != nil {
			return nil
		}
		if relDir == "." {
			relDir = ""
		}

		lines := strings.Split(string(data), "\n")
		gi := ignore.CompileIgnoreLines(lines...)
		m.matchers = append(m.matchers, scopedMatcher{
			dirPrefix: relDir,
			ignore:    gi,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *gitignoreMatcher) isIgnored(relPath string, isDir bool) bool {
	if m == nil || len(m.matchers) == 0 {
		return false
	}

	checkPath := relPath
	if isDir {
		checkPath = relPath + "/"
	}

	for _, sm := range m.matchers {
		var pathToCheck string
		if sm.dirPrefix == "" {
			pathToCheck = checkPath
		} else {
			prefix := sm.dirPrefix + "/"
			if !strings.HasPrefix(relPath, prefix) {
				continue
			}
			pathToCheck = strings.TrimPrefix(checkPath, prefix)
		}

		if sm.ignore.MatchesPath(pathToCheck) {
			return true
		}
	}
	return false
}
