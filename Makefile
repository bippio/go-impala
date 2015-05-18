
thrift:
	thrift -r -gen go:package_prefix=github.com/koblas/impalathing/services/ interfaces/ImpalaService.thrift
	rm -rf ./services
	mv gen-go services
