# format as follows
# high bytes are drive id
# low bytes are rowid

import datetime

bitsPerDate = 13  # I have mistakenly converted the rowids into this format
#bitsPerDate = 14
bitsPerDriveId = 18
# 14+18=32
maxOrd = 2 ** bitsPerDate - 1

maxDriveId = 2 ** bitsPerDriveId - 1

offset = datetime.datetime(2012, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
dayOffset = int(offset.timestamp() / 3600 / 24)
maxDate = offset + datetime.timedelta(days=maxOrd)


def dateTimeFromOrd(ordinal):
	return offset + datetime.timedelta(days=ordinal)


def dayToOrd(date):
	return date - dayOffset


def dayFromOrd(ordinal):
	return ordinal + dayOffset


def decode(oid):
	return {
		"driveId": oid >> bitsPerDate,
		"day": dayFromOrd((oid & maxOrd))
	}


def encode(driveId, day):
	return driveId << bitsPerDate | dayToOrd(day)


def sqlDateToOrd(date="`date`"):
	return "(" + str(date) + " - " + str(dayOffset) + ")"


def sqlDateFromOrd(ordinal="`ord`"):
	return "(" + ordinal + " + " + str(dayOffset) + ")"


def sqlOrdFromOid(ordinal="`ord`", oid="`oid`"):
	return "(" + oid + " & " + str(maxOrd) + ")" + (" as " + ordinal if ordinal else "")


def sqlDateFromOid(date="`date`", oid="`oid`"):
	return sqlDateFromOrd(sqlOrdFromOid(None, oid)) + (" as " + date if date else "")


def sqlToOidUnoffsetted(driveId="`drive_id`", ordinal="`ord`"):
	return str(driveId) + "<<" + str(bitsPerDate) + " | " + str(ordinal)


def sqlToOid(driveId="`drive_id`", date="`date`", oid="`oid`"):
	return sqlToOidUnoffsetted(driveId, sqlDateToOrd(date)) + (" as " + str(oid) if oid else "")


def sqlDriveIdFromOid(driveId="`drive_id`", oid="`oid`"):
	return "(" + oid + " >> " + str(bitsPerDate) + ")" + (" as " + driveId if driveId else "")


def sqlFromOid(driveId="`drive_id`", date="`date`", ordinal=None, oid="`oid`"):
	config = {
		"driveId": (driveId, sqlDriveIdFromOid),
		"date": (date, sqlDateFromOid),
		"ordinal": (ordinal, sqlOrdFromOid)
	}
	loc = locals()
	return ", ".join((func(arg, oid) for varName, (arg, func) in config.items() if loc[varName]))


def sqlThisDrive(driveId=":drive", oid="`oid`", minOrd=0, maxOrd=maxOrd):
	return "(" + oid + " >= (" + sqlToOidUnoffsetted(driveId, minOrd) + ") and " + oid + " <= (" + sqlToOidUnoffsetted(driveId, maxOrd) + "))"
