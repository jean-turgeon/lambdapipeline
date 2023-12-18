FUNCTIONS := handler
STACK_NAME := population-processing
ARCH := aarch64-unknown-linux-gnu


build:
	rm -rf ./build
	cross build --release --target $(ARCH)
	mkdir -p ./build
	${MAKE} ${MAKEOPTS} $(foreach function,${FUNCTIONS}, build-${function})

build-%:
	mkdir -p ./build/$*
	cp -v ./target/$(ARCH)/release/$* ./build/$*/bootstrap

deploy:
	sam deploy --guided --no-fail-on-empty-changeset --no-confirm-changeset --profile test --stack-name ${STACK_NAME}-lambda --template-file ./ci/lambda.yml

delete:
	sam delete --profile test --stack-name ${STACK_NAME}-lambda
