Backblaze dataset analytics scripts [![Unlicensed work](https://raw.githubusercontent.com/unlicense/unlicense.org/master/static/favicon.png)](https://unlicense.org/)
====================================
~~![GitLab Build Status](https://gitlab.com/KOLANICH/backblaze_analytics/badges/master/pipeline.svg)~~
~~![GitLab Coverage](https://gitlab.com/KOLANICH/backblaze_analytics/badges/master/coverage.svg)~~
[![Libraries.io Status](https://img.shields.io/librariesio/github/KOLANICH/backblaze_analytics.svg)](https://libraries.io/github/KOLANICH/backblaze_analytics)
[![Code style: antiflash](https://img.shields.io/badge/code%20style-antiflash-FFF.svg)](https://codeberg.org/KOLANICH-tools/antiflash.py)

**We have moved to https://codeberg.org/KOLANICH-ML/backblaze_analytics.py , grab new versions there.**

Under the disguise of "better security" Micro$oft-owned GitHub has [discriminated users of 1FA passwords](https://github.blog/2023-03-09-raising-the-bar-for-software-security-github-2fa-begins-march-13/) while having commercial interest in success and wide adoption of [FIDO 1FA specifications](https://fidoalliance.org/specifications/download/) and [Windows Hello implementation](https://support.microsoft.com/en-us/windows/passkeys-in-windows-301c8944-5ea2-452b-9886-97e4d2ef4422) which [it promotes as a replacement for passwords](https://github.blog/2023-07-12-introducing-passwordless-authentication-on-github-com/). It will result in dire consequencies and is competely inacceptable, [read why](https://codeberg.org/KOLANICH/Fuck-GuanTEEnomo).

If you don't want to participate in harming yourself, it is recommended to follow the lead and migrate somewhere away of GitHub and Micro$oft. Here is [the list of alternatives and rationales to do it](https://github.com/orgs/community/discussions/49869). If they delete the discussion, there are certain well-known places where you can get a copy of it. [Read why you should also leave GitHub](https://codeberg.org/KOLANICH/Fuck-GuanTEEnomo).

---

It is a set of scripts to download the [Backblaze dataset](https://www.backblaze.com/b2/hard-drive-test-data.html), [normalize](https://en.wikipedia.org/wiki/Database_normalization), transform its format and analize it.
It's not fully automated, it needs some supervision because BackBlase dataset very often brings surprises badly with anomalies like incorrect or missing data, like nonexisting vendors or missing serial numbers, afterfailure use, multiple failures or changed columns format.

Why to normalize
----------------

1 To shrink size with deduplication.

2 To make structure more convenient (and faster) to analyze.
We surely want to know which makes of drives of which vendors' drives live the longest.

Why to transform
----------------
We apply the following transformations:

* Changing the format the date is stored.
	In the original dataset it is stored as strings. It is wasting of space and slowdown. So we convert it to integer using the following formula:
	```sql
	cast(strftime("%s", `date`)/(3600*24) as int) as `date`
	```
	In human language it means we take UNIX timestamp and divide it by count of seconds in day, so we get count of days passed since UNIC epoch.

*	Packing date and drive id into rowid.
	Our format is defined as following (in a [Kaitai Struct syntax](https://github.com/kaitai-io/kaitai_struct_doc/blob/master/ksy_reference.adoc)):
	```yaml
	seq:
	   - id: drive_id
	     type: b19
	   - id: date
	     type: b13
	instances:
	  day: date+15340
	```
   
	15340 is the offset, corresponding to `2012-01-01`, to pack the days numbers into 13 bit.

	13 bits should be enough until 2023

	`13 + 19=32 bit`

	Profits against `rowid` tables where `date` and `drive_id` are separate columns: 

	* data is stored linearly in order (drive_1_day_1, drive_1_day_2, ... drive_2_day_1, drive_2_day_2), so cheap fetches for a single drive, and cheap max and min values

	* no indexes -> no slowdown on update from reindexing (but may be slowdown due to gaps in rowids)

	* less size

	* far more faster statistics computation
		
	Profits against `WITHOUT ROWID` tables where `date` and `drive_id` are separate columns with compound primary key on them:

	* `WITHOUT ROWID` tables use b-tree instead of b*-tree - so the data is stored more optimally

	Drawbacks:

	* no such columns, so no constraints validation

	* very slow import, `~ 95 ops/second`, [see throughput_from_batch_size_dependence.ipynb](./throughput_from_batch_size_dependence.ipynb) for more details

Problems encountered
==================
* [SQLite](https://www.sqlite.org) is damn slow:

  * python-mediated approach to find minimum and maximum dates of 6191 failed drives' records took 3:59:32 (on HDD, in-memory (if you have enough) or SSD should have been faster). It's damn terrible.

  * python-mediated approach to find minimum and maximum dates of the rest of drives' records took nearly a week

  * other queries are also damn slow

   The solution are packed rowids, which allow to signifiucantly reduce time for that operation

How to use
==========

The hard way
---------------------

0. Make sure that you have [```7z```](https://sourceforge.net/projects/sevenzip/files/7-Zip/), [```sqlite```](https://www.sqlite.org), [```aria2c```](https://github.com/aria2/aria2/releases), [python3](https://www.python.org/downloads/) and at least ~50 GiB free space (the unpacked raw db is ~ 25 GiB, `VACUUM;` requires twice the size of db) on the disk where the dataset is to be placed.

1. Clone the repo.

2. `python3 -m backblaze_analytics import retrieve > retrieve.cmd`
  this would create a script downloading the datasets from Backblaze website.
  use `--incremental` to download only the datasets which are not in the base. It gets the last rowid in the DB and extracts the date from it, and then filters the datasets on the website using this date.

3. inspect the file and make sure that all the commands are needed (if you update the dataset you should remove the commands downloading the already present info), that the file names look logical with respect to date (Backblaze may change HTML code on the page which can cause failure to recognize the items) and all the tools used are present in the system

4. run it with shell (should work both on Windows and \*nix)

this should download the zipped datasets from BackBlaze website

5. `python3 -m backblaze_analytics import genScript > import.cmd`

6. inspect the file and make sure that all the commands are needed and all the tools used are present in the system

7. run it with shell

this should unzip each csv file of the dataset and import it into `db.sqlite` with `sqlite` command line utility

8. `python3 -m backblaze_analytics import normalizeModels`

 this should put the drives and information about them into a separate table

9. If the DB was previously used, check the description if backblaze have added a new column, and add it and its number to `datasetDescription.py` and `SMARTAttrsNames.py`. Then run `python3 -m backblaze_analytics import upgradeSchema`.

 This should check if all the needed columns present in the DB and would add them into stats table.

10. `python3 -m backblaze_analytics import normalizeRecords`

  this should move the records changing their structure: removing the data constant for the same drive and packing the data into rowid

11. `python3 -m backblaze_analytics preprocess`

  this should find each drive's lifespan, failed drives and anomalies and put it into `analytics.sqlite`

12. `python3 -m backblaze_analytics export drives`
  this should create a small DB with drives, so you don't need the large DB to do analytics on their lifespan, only 2 small DBs: `drives.sqlite` and `analytics.sqlite`

13. `python3 -m backblaze_analytics export dataset`
  this should create a script to compress the main DB with `7zip`. The compression is ~ 20 times.

14. `python3 -m backblaze_analytics imput train`

  this should train the XGBoost model for imputation of missing data. If something goes wrong **CLEAR THE CACHES IN `cache` DIR**.

15. `python3 -m backblaze_analytics imput imput`

  this should imput the missing data using the trained model


16. `python3 -m backblaze_analytics analysis plotLifeLines`

  this should plot lifelines. You can tune the output format and choose the attrs used for stratification.

The easy way
----------------------

1. Clone the repo

2. Download the prepared databases `db.sqlite` (or `drives.sqlite`) and `analytics.sqlite` and unpack them with `xz` or `7z`

3. begin from the step 14 (imputing)
