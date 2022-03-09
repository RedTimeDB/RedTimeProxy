### Makefile ---
IMAGE=RedTimeProxy

build:
	go build -mod vendor

dep:
	go mod vendor

release:
	CGO_ENABLED=0 go build -mod vendor

container: release
	docker build -t $(IMAGE) .

### Makefile ends here
