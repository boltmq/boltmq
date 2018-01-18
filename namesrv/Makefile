.PHONY: all deps compile test cover clean

OUTDIR=.
OUTBIN=name-server

all:compile

compile:
	@go build -o $(OUTDIR)/$(OUTBIN) main.go
	@echo "namesrv compile success -> $(OUTDIR)/$(OUTBIN)."

deps:
	@go get -v ./... 

test:

cover:

clean:
	@rm -fr $(OUTDIR)/$(OUTBIN) 
	@echo "clean ok."
