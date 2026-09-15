// quicksilver-plugin is the CNPG-I plugin: a gRPC server the CloudNativePG
// operator dials to validate Cluster resources, inject the mirror sidecar into
// instance Pods, and set the PostgreSQL parameters the mirror needs.
//
// It runs as an ordinary Deployment next to the operator and is reached over
// mutual TLS; cnpg-i-machinery's CreateMainCmd handles the certificates, the
// listener and the graceful shutdown. What is specific to Quicksilver is the
// four services registered below.
//
//	quicksilver-plugin --server-cert ... --server-key ... --client-cert ...
package main

import (
	"fmt"
	"os"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/cnpg-i/pkg/postgres"
	"google.golang.org/grpc"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

func main() {
	cmd := http.CreateMainCmd(qsplugin.IdentityImpl{}, func(server *grpc.Server) error {
		operator.RegisterOperatorServer(server, qsplugin.OperatorImpl{})
		lifecycle.RegisterOperatorLifecycleServer(server, qsplugin.LifecycleImpl{})
		postgres.RegisterPostgresServer(server, qsplugin.PostgresImpl{})
		return nil
	})
	cmd.Use = "quicksilver-plugin"
	cmd.Short = "CNPG-I plugin for pg_quicksilver"

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
