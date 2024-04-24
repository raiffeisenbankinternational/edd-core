define parse_csv
tail -n +2 | sed -e 's/"$$//g' -e 's/^"//g' -e 's/""/"/g'
endef

file ?= $(error Please specify the file=... argument)
pattern ?= $(error Please specify the pattern=... argument)
out ?= $(error Please specify the out=... argument)

lookup:
	gunzip -c ${file} | $(call parse_csv) | grep -F '${pattern}' | jq --slurp


view:
	gunzip -c ${file} | $(call parse_csv) | jq --slurp | less


search:
	gunzip -c ${file} | $(call parse_csv) | jq --slurp | grep -F '${pattern}' -A10 -B10


jsonl:
	gunzip -c ${file} | $(call parse_csv) > ${out}
