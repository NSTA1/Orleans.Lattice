// =============================================================================
// frontdoor.bicepparam - example parameter set for the global ingress lane (F-194).
// -----------------------------------------------------------------------------
// Demonstrates the N-region shape. In a real deployment the coordinator invokes
// modules/frontdoor.bicep from main.bicep with the head FQDNs taken from each
// compute[i].outputs.{explorerFqdn,mcpFqdn,siloStateApiFqdn} (see the WIRING
// RECIPE in the module header); the placeholder FQDNs below only let this params
// file stand alone for `az bicep build-params` demonstration. Front Door is a
// GLOBAL resource - this single profile fronts every region listed here.
// =============================================================================

using '../modules/frontdoor.bicep'

param baseName = 'lattice'

// Two regions here; the same file scales to N by editing this array only. Real
// deploys map these FQDNs from the compute outputs - the values below are the
// deterministic ACA ingress FQDN shape only as a stand-alone placeholder.
param origins = [
  {
    regionCode: 'weu'
    explorerFqdn: 'lattice-explorer-weu.example.azurecontainerapps.io'
    mcpFqdn: 'lattice-mcp-weu.example.azurecontainerapps.io'
    siloStateApiFqdn: 'lattice-silo-weu.example.azurecontainerapps.io'
  }
  {
    regionCode: 'eus2'
    explorerFqdn: 'lattice-explorer-eus2.example.azurecontainerapps.io'
    mcpFqdn: 'lattice-mcp-eus2.example.azurecontainerapps.io'
    siloStateApiFqdn: 'lattice-silo-eus2.example.azurecontainerapps.io'
  }
]

// WAF is descoped from the baseline for cost. Leave this false; see the module
// header for the one-config-change Premium + managed-WAF enablement path.
param enableWaf = false
