package options

import (
	"github.com/BenjaminGlusa/goktm/pkg/model"
	"gopkg.in/yaml.v2"
	"os"
	"path/filepath"
)

func ParseJob(pathToYaml string) *model.Job {
	filename, err := filepath.Abs(pathToYaml)
	if err != nil {
		panic("could not open yaml: " + err.Error())
	}
	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		panic("Could not open yaml: " + err.Error())
	}

	var job model.Job
	err = yaml.Unmarshal(yamlFile, &job)
	if err != nil {
		panic("Could not parse yaml: " + err.Error())
	}
	return &job
}
