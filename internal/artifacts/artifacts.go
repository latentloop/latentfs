package artifacts

import _ "embed"

// Global artifacts

//go:embed global/settings.yaml
var GlobalSettings []byte

//go:embed global/project_config.yaml
var ProjectConfig []byte
