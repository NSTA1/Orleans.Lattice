# Default parameters for the azure-throughput benchmark VM.
# Copy to parameters.local.ps1 and edit. The .local.ps1 form is gitignored.

@{
	SubscriptionId          = ''                       # az account show --query id -o tsv
	ResourceGroup           = 'rg-lat'
	Location                = 'westus3'                # set to the same region as the Tables account
	NamePrefix              = 'lat'
	VmSize                  = 'Standard_D2as_v5'       # smallest SKU with accelerated networking.
													   # See §26 / §30 of benchmark/azure-throughput/throughput.md
													   # for the empirically-derived SKU sizing rule on this
													   # benchmark. Summary:
													   #   D2as_v5 (2 vCPU): UNDER-provisioned at 4k:5.
													   #     Silo + co-located producer pin both cores;
													   #     run 3 of the n=3 baseline collapsed to a
													   #     degraded cohort (failed=24,576).
													   #   D4as_v5 (4 vCPU): SWEET SPOT for the 4k:5 rung.
													   #     Silo CPU ~55-75% of box at peak under the
													   #     shipped WAL defaults; clean n=3 cohorts.
													   #     Recommended floor for any cycle that wants
													   #     drain-bound (rather than CPU-bound) signal.
													   #   D8as_v5 (8 vCPU): use when chasing the silo's
													   #     true post-cap=16 ceiling at 6k:5 or above,
													   #     or for any rung that needs headroom for a
													   #     CPU-confound check. The F8as_v6 SKU tried
													   #     2026-06-04 was 73% idle at 4k:5 (silo CPU
													   #     ~28% of box), so D8 is the right knob for
													   #     headroom rather than F8.
													   # Decision rule (§26.2): pick the smallest SKU where
													   # silo CPU avg sits 40-75% of box AND system CPU peak
													   # stays <90% AND failed=0 across n=3 at the target rung.
													   # Override via -VmSize if you have a CPU-bound
													   # workload (rare for this benchmark).
	AdminUsername           = 'azureuser'
	SshPublicKeyPath        = "$HOME/.ssh/id_ed25519.pub"
	AllowedSshSourceAddress = ''                       # e.g. 203.0.113.4/32; leave blank to auto-detect
	AutoShutdownTimeZone    = 'UTC'
	AutoShutdownTime        = '1900'                   # 19:00 UTC
	OsDiskSizeGB            = 64
}
