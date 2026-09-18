/*
Copyright 2026 The llm-d-stream-handler-plugin Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command epp is llm-d's Endpoint Picker with the stream handler provisioner
// plugin compiled in.
//
// A custom binary is the supported way to add an out-of-tree plugin: the
// framework's plugin registry is a package-level map, so a plugin type only
// exists for configurations parsed by a process that registered it. Everything
// else here is stock llm-d -- the runner owns flag parsing, config loading and
// the whole server lifecycle, and registers its own in-tree plugins while it
// processes the configuration.
package main

import (
	"os"

	"github.com/llm-d/llm-d-router/cmd/epp/runner"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/Tanchwa/llm-d-stream-handler-plugin/pkg/streamhandler"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx := ctrl.SetupSignalHandler()

	// Registering before Run is safe: Register only writes to the registry map,
	// and the runner's own registration uses distinct type names, so neither
	// overwrites the other.
	fwkplugin.Register(streamhandler.PluginType, streamhandler.Factory)

	if err := runner.NewRunner().Run(ctx); err != nil {
		return 1
	}
	return 0
}
