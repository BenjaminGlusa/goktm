package main

import (
	"flag"
	"fmt"
	"github.com/BenjaminGlusa/goktm/pkg/sink"
	"github.com/BenjaminGlusa/goktm/pkg/source"

	//"github.com/BenjaminGlusa/goktm/internal/operations"
	"github.com/BenjaminGlusa/goktm/internal/options"
	"github.com/BenjaminGlusa/goktm/pkg/model"
)

func main() {
	fmt.Println("Hello")
	job := parseJob()

	messageSink := sink.SinkFactory(job)
	messageSource := source.SourceFactory(job)

	messageSource.Fetch(messageSink)
}

func parseJob() *model.Job {
	var optionsYaml string
	flag.StringVar(&optionsYaml, "options-yaml", "", "path to the options yaml")

	flag.Parse()

	if len(optionsYaml) == 0 {
		panic("no options-yaml provided.")
	}

	return options.ParseJob(optionsYaml)
}
