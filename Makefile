
thrift:
	thrift -r -gen go:package_prefix=github.com/bippio/go-impala/services/ interfaces/ImpalaService.thrift
	rm -rf ./services
	mv gen-go services
