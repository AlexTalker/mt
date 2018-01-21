
DATA_URL := ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz

data:
	gzip -cd data.gz > $@

data.gz:
	curl -L "$(DATA_URL)" > $@
