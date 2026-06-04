# Default parameters for the lattice-wedge VM deployment.
# Copy to vm.parameters.local.ps1 and edit. The .local.ps1 form is gitignored.

@{
	SubscriptionId          = ''                       # az account show --query id -o tsv
	ResourceGroup           = 'rg-lat'
	Location                = 'westus3'                # set to the same region as the Tables account
	NamePrefix              = 'lat'
	VmSize                  = 'Standard_D2as_v5'       # smallest SKU with accelerated networking.
													   # F8as_v6 (8 vCPU AMD Zen4) was tried 2026-06-04
													   # for the throughput sweep but the silo never used
													   # more than ~5 of 8 cores even at 25k:5 saturation,
													   # so we're paying ~2x for headroom we don't touch.
													   # Override via -VmSize if you have a CPU-bound
													   # workload (rare for this benchmark).
	AdminUsername           = 'azureuser'
	SshPublicKeyPath        = "$HOME/.ssh/id_ed25519.pub"
	AllowedSshSourceAddress = ''                       # e.g. 203.0.113.4/32; leave blank to auto-detect
	AutoShutdownTimeZone    = 'UTC'
	AutoShutdownTime        = '1900'                   # 19:00 UTC
	OsDiskSizeGB            = 64
}
