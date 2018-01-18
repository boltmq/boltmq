.PHONY: all deps compile config test cover clean

CURDIR=$(realpath .)
OUTDIR=$(CURDIR)/bin
SRVDIR=$(OUTDIR)/service
BRKDIR=$(CURDIR)/broker
NMSDIR=$(CURDIR)/namesrv

all:compile config
	@echo "\nrun: cd $(OUTDIR) && ./broker start"

compile:
	@make -C broker OUTDIR=$(SRVDIR)
	@make -C namesrv OUTDIR=$(SRVDIR)
	@cp -f $(CURDIR)/scripts/* $(OUTDIR)/ 

config:
	@mkdir -p $(OUTDIR)/etc
	@cp -f $(BRKDIR)/etc/broker.sample.toml $(OUTDIR)/etc/broker.toml 
	@cp -f $(BRKDIR)/etc/seelog.sample.xml $(OUTDIR)/etc/seelog-broker.xml
	@cp -f $(NMSDIR)/etc/namesrv.sample.toml $(OUTDIR)/etc/namesrv.toml 
	@cp -f $(NMSDIR)/etc/seelog.sample.xml $(OUTDIR)/etc/seelog-namesrv.xml
	@mkdir -p $(OUTDIR)/logs
	@echo "\nBoltMQ config dir is $(OUTDIR)/etc, if you need you can edit it."

deps:
	@make -C broker deps
	@make -C namesrv deps

test:

pack:
	#@tar zcf broker.tar.gz $(OUTDIR)

cover:

clean:
	@rm -fr $(OUTDIR)
	@echo "clean ok."
