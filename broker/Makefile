.PHONY: all deps compile test cover clean

OUTDIR=.
OUTBIN=broker

all:compile

compile:
	@go build -o $(OUTDIR)/$(OUTBIN) main.go
	@echo "broker compile success -> $(OUTDIR)/$(OUTBIN)."

deps:
	@go get -v ./... 

test:

cover:

clean:
	@rm -fr $(OUTDIR)/$(OUTBIN) 
	@echo "clean ok."
