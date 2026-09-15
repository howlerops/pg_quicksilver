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
	services := []identity.PluginCapability_Service_Type{
		identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE,
		identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE,
		identity.PluginCapability_Service_TYPE_POSTGRES,
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
