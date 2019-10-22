.PHONY: clean
clean:
	rm -rf vendor/

.PHONY: vendor
vendor: clean
	go mod vendor