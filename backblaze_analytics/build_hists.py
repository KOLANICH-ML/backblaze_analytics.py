# this code is obsolete (we use lifelines for now, it computes this hists internally), but may be useful in future for custom processings

modelsHists = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))
for rec in dsDenorm:
	lifespan = rec["failure_worked_days_synthetic"]
	#lifespan = drive["initial_smart_worked_days"] + drive["failure_date"] - drive["first_date"]
	drive = drivesIndex[rec["id"]]
	model = modelsIndex[drive["model_id"]]
	brand = brandsIndex[model["brand_id"]]
	vendor = vendorsIndex[brand["vendor_id"]]
	modelsHists[vendor["name"]][brand["name"]][model["name"]][lifespan] += 1

brandHists = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
for vendor in modelsHists:
	for brand in modelsHists[vendor]:
		for model in modelsHists[vendor][brand]:
			for lifespan in modelsHists[vendor][brand][model]:
				brandHists[vendor][brand][lifespan] += modelsHists[vendor][brand][model][lifespan]

vendorHists = defaultdict(lambda: defaultdict(int))
for vendor in brandHists:
	for brand in brandHists[vendor]:
		for lifespan in brandHists[vendor][brand]:
			vendorHists[vendor][lifespan] += brandHists[vendor][brand][lifespan]

overallHist = defaultdict(int)
for vendor in vendorHists:
	for lifespan in vendorHists[vendor]:
		overallHist[lifespan] += vendorHists[vendor][lifespan]
