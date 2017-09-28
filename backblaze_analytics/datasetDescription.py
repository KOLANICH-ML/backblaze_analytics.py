__all__ = ("smartAttrIDs", "existingAttrIDs", "smartAttrsResolvers", "hddAttrsIDs", "spec", "targetAttrsSpec", "attrsSpec")
"""Dataset-specific knowledge used in scripts"""

import itertools

from .SMARTAttrsNames import SMARTAttrsNames, SSDSMARTAttrsIds

r = range
smartAttrIDs = list(itertools.chain(
	r(1, 6), r(7, 14), r(15, 18), r(22, 25), (168, 170, 173, 174, 177, 179), r(181, 185), r(187, 202), (218, 220), r(222, 227), r(231, 234), (235,), r(240, 243), r(250, 253), r(254, 256)
))

del r
existingAttrIDs = [id for id in smartAttrIDs if id in SMARTAttrsNames]
hddAttrsIDs = [id for id in smartAttrIDs if id not in SSDSMARTAttrsIds]


class smartAttrsResolvers:
	def basic(smartAttrIDs):
		return ("smart_" + str(attrNum) for attrNum in smartAttrIDs)

	def pretty(smartAttrIDs):
		return (SMARTAttrsNames[attrNum] if attrNum in SMARTAttrsNames else basic(attrNum) for attrNum in smartAttrIDs)


b = "binary"
n = "numerical"
c = "categorical"
s = "stop"

targetAttrsSpec = {
	"failed": b,
	"duration_worked": n,
}

attrsSpec = {
	#"days_in_dataset": b,
	#"days_in_dataset_failure": n,
	#"failure_date": n,
	#"first_date": n,
	"id": s,
	"last_date": s,
	"model_id": s,
	#"brand_id": c,
	#"vendor_id": c,
	"brand_id": s,
	"vendor_id": s,
	"vendor": s,
	"series": c,
	"Scorpio": b,
	"Caviar": b,
	"GP": b,
	"Red": b,
	#"N2": s,
	"N2": b,
	#"SpinPoint": s,
	"SpinPoint": b,
	"buffer_size": n,
	"capacity": n,
	#"top_capacity": n,
	"top_capacity": s,
	"interface": s,
	#"interface": c,
	#"interface_version": s,
	"interface_version": n,
	"interface_speed": n,
	"interface_pin_count": n,
	#"form_factor": s,
	"form_factor": n,
	"form_factor_weight": n,
	"form_factor_depth": n,
	"form_factor_width": n,
	"form_factor_height": n,
	"segment": c,
	"family": s,
	"sector_size": n,
	"feature_code": s,
	"rpm": n,
	"data_security_mode": s,
	"platters": n,
	"max_platters": s,
	"variable_rpm": b,
	"first_known_date": n,
	#"feature": c,
	"feature": s,
	"name": s,
	"attributes": s,
	"generation_code": s,
	"options": s,
	"type": s,
	"comment": s,
	"product_code": s,
}

spec = {}
spec.update(targetAttrsSpec)
spec.update(attrsSpec)
