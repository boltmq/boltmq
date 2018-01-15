.PHONY: all deps compile config test cover clean

CURDIR=$(realpath .)
OUTDIR=$(CURDIR)/bin
BRKDIR=$(CURDIR)/broker

all:compile config
	@echo "\nrun: cd $(OUTDIR) && ./broker"

compile:
	@make -C broker OUTDIR=$(OUTDIR)

config:
	@mkdir -p $(OUTDIR)/etc
	@cp -f $(BRKDIR)/etc/broker.sample.toml $(OUTDIR)/etc/broker.toml 
	@cp -f $(BRKDIR)/etc/seelog.sample.xml $(OUTDIR)/etc/seelog-broker.xml
	@mkdir -p $(OUTDIR)/logs
	@echo "move config file to $(OUTDIR)/etc, if you need you can edit it."

deps:

test:

pack:
	#@tar zcf broker.tar.gz $(OUTDIR)

cover:

clean:
	@rm -fr $(OUTDIR)
	@echo "clean ok."
