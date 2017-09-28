--
-- Create a table where the column names exactly match
-- the column names in the published CSV files.  This is for the format beginning in 2015.
--

create TABLE 'vendors' (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	UNIQUE(name) ON CONFLICT REPLACE
);

create TABLE 'brands' (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	vendor_id INTEGER,
	model_name_regex TEXT,
	UNIQUE(name) ON CONFLICT REPLACE,
	FOREIGN KEY('vendor_id') REFERENCES vendors('id')
);

INSERT INTO `vendors` (`id`, `name`) VALUES (0, 'Unknown');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (0, 'Unknown', '');
INSERT INTO `vendors` (`id`, `name`) VALUES (1, 'Seagate');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (1, 'Seagate', 'ST\d+(?:[A-Z]{2}\d+|AS)(\s[A-Z]{2})?');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (1, 'Samsung', 'H[DAEMS]\d{3}[A-Z]{2}(?:/p)?|[SM][PV]\d{4}[A-Z](?:/r)?|(7(PA|PC|TD|TE|WD)|M(P[AC]|TD)|N(LN|TD)|CBQ|C[QR]E|DOE|HPV|YTY)(28G|024|032|E32|56G|64G|256|128|256|480|512)([5F]M[PXU]P|H[MABCD](CD|DR|F[UV]|G[LMR]|H[PQ]|JP|LH))');
INSERT INTO `vendors` (`id`, `name`) VALUES (2, 'Western Digital');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (2, 'Western Digital', '(?:WDC )?WD\d+(?:[A-X][0-29A-Z])?[26A-Z][A-Z]');
INSERT INTO `vendors` (`id`, `name`) VALUES (3, 'Toshiba');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (3, 'Toshiba', 'TOSHIBA.+(AL|DT|M[DGCNQ]|PX|THN)\w{2}[ASPRC]([ABCEX]|[HLMRV])[ABDFQU]\d{2,3}[0DE]?([ANE]Y|VS?|[BCHPQWDFGR])?|M[BJH]\w{2}[23]\d{3}(A[CHT]|B[HJKS]|C[HJ]|F[CD]|N[PC]|RC)');
INSERT INTO `vendors` (`id`, `name`) VALUES (4, 'Hitachi');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (4, 'HGST', '^HGST [WH][UDTECM][HSCEATNP](\d{2}|5C)\d{4}[PVDKABCHJLMSG][L795S]([13][68]|F[24]|A[T3]|SA|[AENS]6|SS|[45]2)[0-486][01245]');
	INSERT INTO `brands` (`vendor_id`, `name`, `model_name_regex`) VALUES (4, 'Hitachi', '^Hitachi H[UDTECM][HSCEATNP](\d{2}|5C)\d{4}[PVDKABCHJLMSG][L795S]([13][68]|F[24]|A[T3]|SA|[AENS]6|SS|[45]2)[0123486][01245]');


CREATE TABLE 'models' (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	brand_id INTEGER,
	UNIQUE(name) ON CONFLICT REPLACE,
	FOREIGN KEY('brand_id') REFERENCES brands('id')
);

CREATE TABLE "drives" (
	id INTEGER PRIMARY KEY,
	model_id INTEGER,
	serial_number TEXT NOT NULL,
	UNIQUE(serial_number) ON CONFLICT REPLACE,
	FOREIGN KEY('model_id') REFERENCES models('id')
);

ATTACH database "./analytics.sqlite" as analytics;

CREATE TABLE analytics."anomalies"(
	id INTEGER,
	info TEXT,
	PRIMARY KEY('id') ON CONFLICT REPLACE,
	FOREIGN KEY('id') REFERENCES drives('id')
);

CREATE TABLE analytics."drives_analytics" (
	id INTEGER,
	first_date INTEGER,
	last_date INTEGER,
	failure_date INTEGER,
	PRIMARY KEY('id') ON CONFLICT REPLACE,
	FOREIGN KEY('id') REFERENCES drives('id')
);

CREATE TABLE analytics."censored_drives" (
	id INTEGER,
	PRIMARY KEY('id') ON CONFLICT REPLACE,
	FOREIGN KEY('id') REFERENCES drives('id')
);