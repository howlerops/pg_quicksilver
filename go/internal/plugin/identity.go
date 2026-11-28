package plugin

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
)

// Version is stamped at build time with -ldflags.
var Version = "dev"

// IdentityImpl answers the three RPCs CNPG uses to discover what this plugin is
// and which services it actually serves. Declaring a capability we do not
// implement makes the operator call a method that returns Unimplemented, so the
// list below is kept exactly in step with the services registered in
// cmd/quicksilver-plugin.
type IdentityImpl struct {
	identity.UnimplementedIdentityServer
}

func (IdentityImpl) GetPluginMetadata(
	context.Context, *identity.GetPluginMetadataRequest,
) (*identity.GetPluginMetadataResponse, error) {
	return &identity.GetPluginMetadataResponse{
		Name:          PluginName,
		Version:       Version,
		DisplayName:   "Quicksilver",
		Description:   "Replaces read replicas with a columnar mirror of the primary built from the WAL",
		ProjectUrl:    "https://github.com/howlerops/pg_quicksilver",
		RepositoryUrl: "https://github.com/howlerops/pg_quicksilver",
		License:       "Apache-2.0",
		LicenseUrl:    "https://github.com/howlerops/pg_quicksilver/blob/main/LICENSE",
		Maturity:      "alpha",
		Vendor:        "howlerops",
	}, nil
}

func (IdentityImpl) GetPluginCapabilities(
	context.Context, *identity.GetPluginCapabilitiesRequest,
) (*identity.GetPluginCapabilitiesResponse, error) {
	// Deliberately NOT advertised:
	//   TYPE_POSTGRES. The instance manager never calls this plugin for
	//   EnrichConfiguration, so advertising it promised a parameter write that
	//   never happened (postgres.go).
	//   TYPE_INSTANCE_SIDECAR_INJECTION. Advertising it would put this plugin in
	//   every instance reconcile's list (api/v1 GetInstanceEnabledPluginNames).
	//   CloudNativePG 1.27.0 then dials a socket in the Pod and fails the whole
	//   reconcile, promotion included, when it is missing (instance_controller.go
	//   WithPlugins). 1.30 filters on the sockets present; 1.27 does not.
	services := []identity.PluginCapability_Service_Type{
		identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE,
		identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE,
	}
	caps := make([]*identity.PluginCapability, 0, len(services))
	for _, s := range services {
		caps = append(caps, &identity.PluginCapability{
			Type: &identity.PluginCapability_Service_{
				Service: &identity.PluginCapability_Service{Type: s},
			},
		})
	}
	return &identity.GetPluginCapabilitiesResponse{Capabilities: caps}, nil
}

func (IdentityImpl) Probe(
	context.Context, *identity.ProbeRequest,
) (*identity.ProbeResponse, error) {
	return &identity.ProbeResponse{Ready: true}, nil
}
