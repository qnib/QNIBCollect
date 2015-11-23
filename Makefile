FULLERITE      := fullerite
BEATIT         := beatit
VERSION        := 0.1.16
SRCDIR         := src
HANDLER_DIR    := $(SRCDIR)/fullerite/handler
PROTO_SFX      := $(HANDLER_DIR)/signalfx.proto
GEN_PROTO_SFX  := $(HANDLER_DIR)/signalfx.pb.go
PKGS           := \
	$(BEATIT) \
	$(FULLERITE) \
	$(FULLERITE)/collector \
	$(FULLERITE)/config \
	$(FULLERITE)/handler \
	$(FULLERITE)/internalserver\
	$(FULLERITE)/metric

SOURCES        := $(foreach pkg, $(PKGS), $(wildcard $(SRCDIR)/$(pkg)/*.go))
SOURCES        := $(filter-out $(GEN_PROTO_SFX), $(SOURCES))
OS	       := $(shell /usr/bin/lsb_release -si 2> /dev/null)

space :=
space +=
comma := ,

# symlinks confuse go tools, let's not mess with it and use -L
GOPATH  := $(shell pwd -L)
export GOPATH

PATH := bin:$(PATH)
export PATH

all: clean fmt lint $(FULLERITE) $(BEATIT) test

.PHONY: clean
clean:
	@echo Cleaning $(FULLERITE)...
	@rm -f $(FULLERITE) bin/$(FULLERITE)
	@rm -f $(BEATIT) bin/$(BEATIT)
	@rm -rf pkg/*/$(FULLERITE)
	@rm -rf build fullerite*.deb fullerite*.rpm
# Let's keep the generated file in the repo for ease of development.
#	@rm -f $(GEN_PROTO_SFX)

deps:
	@echo Getting dependencies...
	@$(foreach pkg, $(PKGS), go get -t $(pkg);)

$(FULLERITE): $(SOURCES) deps
	@echo Building $(FULLERITE)...
	@go build -o bin/$(FULLERITE) $@

$(BEATIT): $(BEATIT_SOURCES)
	@echo Building $(BEATIT)...
	@go build -o bin/$(BEATIT) $@

test: tests
tests: deps
	@echo Testing $(FULLERITE)
	@$(foreach pkg, $(PKGS), go test -cover $(pkg);)

coverage_report: deps
	@echo Creating a coverage rport for $(FULLERITE)
	@$(foreach pkg, $(PKGS), go test -coverprofile=coverage.out -coverpkg=$(subst $(space),$(comma),$(PKGS)) $(pkg);)
	@go tool cover -html=coverage.out



fmt: $(SOURCES)
	@$(foreach pkg, $(PKGS), go fmt $(pkg);)

vet: $(SOURCES)
	@echo Vetting $(FULLERITE) sources...
	@go get -d -u golang.org/x/tools/cmd/vet
	@$(foreach pkg, $(PKGS), go vet $(pkg);)

proto: protobuf
protobuf: $(PROTO_SFX)
	@echo Compiling protobuf
	@go get -u github.com/golang/protobuf/proto
	@go get -u github.com/golang/protobuf/protoc-gen-go
	@protoc --go_out=. $(PROTO_SFX)

lint: $(SOURCES)
	@echo Linting $(FULLERITE) sources...
	@go get -u github.com/golang/lint/golint
	@$(foreach src, $(SOURCES), bin/golint $(src);)

cyclo: $(SOURCES)
	@echo Checking code complexity...
	@go get -u github.com/fzipp/gocyclo
	@bin/gocyclo $(SOURCES)

pkg: package
package: clean $(FULLERITE) $(BEATIT)
	@echo Packaging for $(OS)
	@mkdir -p build/usr/bin build/usr/share/fullerite build/etc
	@cp bin/fullerite build/usr/bin/
	@cp bin/beatit build/usr/bin/
	@cp deb/bin/run-* build/usr/bin/
	@cp fullerite.conf.example build/etc/
	@cp -r src/diamond build/usr/share/fullerite/diamond
ifeq ($(OS),Ubuntu)
	@fpm -s dir \
		-t deb \
		--name $(FULLERITE) \
		--version $(VERSION) \
		--description "metrics collector" \
		--depends python \
		--deb-user "fullerite" \
		--deb-group "fullerite" \
		--deb-default "deb/etc/fullerite" \
		--deb-upstart "deb/etc/init/fullerite" \
		--deb-upstart "deb/etc/init/fullerite_diamond_server" \
		--before-install "deb/before_install.sh" \
		--before-remove "deb/before_rm.sh" \
		-C build .
# CentOS 7 Only
else ifeq ($(OS),CentOS)
	@fpm -s dir \
		-t rpm \
		--name $(FULLERITE) \
		--version $(VERSION) \
		--description "metrics collector" \
		--depends python \
		--rpm-user "fullerite" \
		--rpm-group "fullerite" \
                --before-install "rpm/before_install.sh" \
		--before-remove "rpm/before_rm.sh" \
		-C build . \
		../rpm/fullerite.systemd=/etc/systemd/system/fullerite.service \
                ../rpm/fullerite.sysconfig=/etc/sysconfig/fullerite
else
	@echo "OS not supported"
endif
